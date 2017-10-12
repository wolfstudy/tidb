package server

import (
	"github.com/pingcap/tipb/go-mysqlx/Crud"
	"github.com/pingcap/tidb/xprotocol/util"
	log "github.com/Sirupsen/logrus"
	"github.com/pingcap/tidb/xprotocol/expr"
)

type insertBuilder struct {}

func (ib *insertBuilder) build(payload []byte) (*string, error) {
	var msg Mysqlx_Crud.Insert
	var generatedField *string
	var err error
	var isRelation bool

	if err := msg.Unmarshal(payload); err != nil {
		return nil, util.ErXBadMessage
	}

	projectionSize := 1
	if msg.GetDataModel() == Mysqlx_Crud.DataModel_TABLE {
		isRelation = true
		projectionSize = len(msg.Projection)
	}

	sqlQuery := "INSERT INTO "
	sqlQuery += *ib.addCollection(msg.Collection)
	generatedField, err = ib.addProjection(msg.Projection, isRelation)
	if err != nil {
		return nil, err
	}
	sqlQuery += *generatedField

	generatedField, err = ib.addValues(msg.Row, projectionSize)
	if err != nil {
		return nil, err
	}
	sqlQuery += *generatedField

	return &sqlQuery, nil
}


func (ib *insertBuilder)addCollection(c *Mysqlx_Crud.Collection) *string {
	target := util.QuoteIdentifier(*c.Schema)
	target += "."
	target += util.QuoteIdentifier(*c.Name)
	return &target
}

func (ib *insertBuilder)addProjection(p []*Mysqlx_Crud.Column, tableDataMode bool) (*string, error) {
	target := ""
	if tableDataMode {
		if len(p) != 0 {
			target += " (" + *p[0].Name
			for _, col := range p {
				target += ","
				target += *col.Name
			}
			target += ")"
		}
	} else {
		if len(p) != 0 {
			return nil, util.ErrorMessage(util.CodeErXBadProjection, "Invalid projection for document operation")
		}
		target += " (doc)"
	}
	return &target, nil
}

func (ib *insertBuilder)addValues(c []*Mysqlx_Crud.Insert_TypedRow, projectionSize int) (*string, error) {
	if len(c) == 0 {
		return nil, util.ErrorMessage(util.CodeErXBadProjection, "Missing row data for Insert")
	}
	target := " VALUES "

	generatedField, err := ib.addRow(c[0], projectionSize)
	if err != nil {
		return nil, err
	}

	target += *generatedField
	for _, row := range c[1:] {
		target += ","
		generatedField, err = ib.addRow(row, projectionSize)
		if err != nil {
			return nil, err
		}
		target += *generatedField
	}

	return &target, nil
}

func (ib *insertBuilder)addRow(row *Mysqlx_Crud.Insert_TypedRow, projectionSize int) (*string, error) {
	if len(row.GetField()) == 0 || len(row.GetField()) != projectionSize {
		log.Infof("[XUWT] row filed(%d), projection size(%d)", len(row.GetField()), projectionSize)
		return nil, util.ErrorMessage(util.CodeErXBadInsertData, "Wrong number of fields in row being inserted")
	}
	target := "("
	generatedField, err := expr.AddExpr(row.GetField()[0])
	if err != nil {
		return nil, err
	}
	target += *generatedField
	for _, field := range row.GetField()[1:] {
		target += ","
		generatedField, err := expr.AddExpr(field)
		if err != nil {
			return nil, err
		}
		target += *generatedField
	}
	target += ")"
	return &target, nil
}
