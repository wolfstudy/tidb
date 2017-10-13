package expr

import (
	"github.com/pingcap/tipb/go-mysqlx/Expr"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	"github.com/pingcap/tidb/xprotocol/util"
	"strconv"
)

type generator interface {
	generate() (*string, error)
}

type expr struct {
	expr *Mysqlx_Expr.Expr
}

func (e *expr)generate() (*string, error) {
	var generator generator

	expr := e.expr
	switch expr.GetType() {
	case Mysqlx_Expr.Expr_IDENT:
		generator = &ident{expr.GetIdentifier()}
	case Mysqlx_Expr.Expr_LITERAL:
		generator = &literal{expr.GetLiteral()}
	case Mysqlx_Expr.Expr_VARIABLE:
		generator = &variable{expr.GetVariable()}
	case Mysqlx_Expr.Expr_FUNC_CALL:
		generator = &funcCall{expr.GetFunctionCall()}
	case Mysqlx_Expr.Expr_OPERATOR:
		generator = &operator{expr.GetOperator()}
	case Mysqlx_Expr.Expr_PLACEHOLDER:
		generator = &placeHolder{expr.GetPosition()}
	case Mysqlx_Expr.Expr_OBJECT:
		generator = &object{expr.GetObject()}
	case Mysqlx_Expr.Expr_ARRAY:
		generator = &array{expr.GetArray()}
	default:
		return nil, util.ErXBadMessage
	}
	return generator.generate()
}

func AddExpr(e interface{}) (*string, error) {
	var generator generator

	switch e.(type) {
	case *Mysqlx_Expr.Expr:
		generator = &expr{e.(*Mysqlx_Expr.Expr)}
	case []*Mysqlx_Expr.DocumentPathItem:
		generator = &docPathArray{e.([]*Mysqlx_Expr.DocumentPathItem)}
	default:
		return nil, util.ErXBadMessage
	}
	return generator.generate()
}

type ident struct {
	identifier *Mysqlx_Expr.ColumnIdentifier
}

func (i *ident) generate() (*string, error) {
	target := ""
	schemaName := i.identifier.GetSchemaName()
	tableName := i.identifier.GetTableName()

	if schemaName != "" && tableName == "" {
		return nil, util.ErrorMessage(util.CodeErXExprMissingArg,
			"Table name is required if schema name is specified in ColumnIdentifier.")
	}

	docPath := i.identifier.GetDocumentPath()
	name := i.identifier.GetName()
	if tableName == "" && name == "" &&
	// @TODO check whether is relation here, see in expr_generator.cc in mysql
		(len(docPath) > 0) {
		return nil, util.ErrorMessage(util.CodeErXExprMissingArg,
			"Column name is required if table name is specified in ColumnIdentifier.")
	}

	if len(docPath) > 0 {
		target += "JSON_EXTRACT("
	}

	if schemaName != "" {
		target += util.QuoteIdentifier(schemaName) + "."
	}

	if tableName != "" {
		target += util.QuoteIdentifier(tableName) + "."
	}

	if name != "" {
		target += util.QuoteIdentifier(name)
	}

	if len(docPath) > 0 {
		if name == "" {
			target += "doc"
		}

		target += ","
		generatedQuery, err := AddExpr(docPath)
		if err != nil {
			return nil, nil
		}
		target += *generatedQuery
		target += ")"
	}
	return &target, nil
}

type literal struct{
	literal *Mysqlx_Datatypes.Scalar
}
func (l *literal) generate() (*string, error) {
	return nil, nil
}

type variable struct{
	variable string
}
func (v *variable) generate() (*string, error) {
	return nil, nil
}

type funcCall struct{
	functionCall *Mysqlx_Expr.FunctionCall
}
func (fc *funcCall) generate() (*string, error) {
	return nil, nil
}

type operator struct{
	operator *Mysqlx_Expr.Operator
}
func (op *operator) generate() (*string, error) {
	return nil, nil
}

type placeHolder struct{
	position uint32
}
func (ph *placeHolder) generate() (*string, error) {
	return nil, nil
}

type object struct{
	object *Mysqlx_Expr.Object
}
func (ob *object) generate() (*string, error) {
	return nil, nil
}

type array struct{
	array *Mysqlx_Expr.Array
}

func (a *array) generate() (*string, error) {
	return nil, nil
}

type docPathArray struct {
	docPath []*Mysqlx_Expr.DocumentPathItem
}

func (d *docPathArray) generate() (*string, error) {
	target := ""

	docPath := d.docPath
	if len(docPath) == 1 &&
		docPath[0].GetType() == Mysqlx_Expr.DocumentPathItem_MEMBER &&
		docPath[0].GetValue() == "" {
		target += util.QuoteIdentifier("$")
		return &target, nil
	}

	target += "\\$"
	for _, item := range docPath {
		switch item.GetType() {
		case Mysqlx_Expr.DocumentPathItem_MEMBER:
			if item.GetValue() == "" {
				return nil, util.ErrorMessage(util.CodeErXExprBadTypeValue,
					"Invalid empty value for Mysqlx::Expr::DocumentPathItem::MEMBER")
			}
			target += util.QuoteIdentifierIfNeeded(item.GetValue())
		case Mysqlx_Expr.DocumentPathItem_MEMBER_ASTERISK:
			target += ".*"
		case Mysqlx_Expr.DocumentPathItem_ARRAY_INDEX:
			target += "[" + strconv.FormatUint(uint64(item.GetIndex()), 10) + "]"
		case Mysqlx_Expr.DocumentPathItem_ARRAY_INDEX_ASTERISK:
			target += "[*]"
		case Mysqlx_Expr.DocumentPathItem_DOUBLE_ASTERISK:
			target += "**"
		default:
			return nil, util.ErrorMessage(util.CodeErXExprBadTypeValue,
				"Invalid value for Mysqlx::Expr::DocumentPathItem::Type ")
		}
	}

	target += "\\"
	return &target, nil
}
