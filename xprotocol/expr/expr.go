package expr

import (
	"github.com/pingcap/tipb/go-mysqlx/Expr"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	"github.com/pingcap/tidb/xprotocol/util"
)

type generator interface {
	generate() (*string, error)
}

func createGenerator(expr *Mysqlx_Expr.Expr) generator {
	switch expr.GetType() {
	case Mysqlx_Expr.Expr_IDENT:
		return &ident{expr.GetIdentifier()}
	case Mysqlx_Expr.Expr_LITERAL:
		return &literal{expr.GetLiteral()}
	case Mysqlx_Expr.Expr_VARIABLE:
		return &variable{expr.GetVariable()}
	case Mysqlx_Expr.Expr_FUNC_CALL:
		return &funcCall{expr.GetFunctionCall()}
	case Mysqlx_Expr.Expr_OPERATOR:
		return &operator{expr.GetOperator()}
	case Mysqlx_Expr.Expr_PLACEHOLDER:
		return &placeHolder{expr.GetPosition()}
	case Mysqlx_Expr.Expr_OBJECT:
		return &object{expr.GetObject()}
	case Mysqlx_Expr.Expr_ARRAY:
		return &array{expr.GetArray()}
	default:
		return nil
	}
}

func AddExpr(expr *Mysqlx_Expr.Expr) (*string, error) {
	generator := createGenerator(expr)
	if generator == nil {
		return nil, util.ErXBadMessage
	}
	return generator.generate()
}

type ident struct{
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
		//generatedQuery, err := AddExpr(docPath)
		target += ")"
	}
	return nil, nil
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
