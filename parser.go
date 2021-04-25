package main

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	sq "github.com/Masterminds/squirrel"
	_ "github.com/lib/pq"
)

var (
	operators     = []string{">=", "<=", "!=", "<>", "=", ">", "<", "LIKE", "ILIKE", "~", "~*", "!~", "!~*"}
	operators_not = []string{"LIKE", "ILIKE"}
	r             = regexp.MustCompile("[a-zA-Z_][a-zA-Z_0-9]*")
)

type step int

const (
	stepNot step = iota
	stepField
	stepOperator
	stepValue
	stepAndOr
)

type Condition struct {
	AndOr    string
	Not      bool
	Field    string
	Operator string
	Value    interface{}
}

func IsOperator(s string) bool {
	for _, o := range operators {
		if strings.ToUpper(s) == o {
			return true
		}
	}
	return false
}

// cheks for AND NOT / OR NOT scenarios
func IsDoubleOperator(s string, sNext string) bool {
	if strings.ToUpper(s) == "NOT" {
		for _, o := range operators_not {
			if strings.ToUpper(sNext) == o {
				return true
			}
		}
	}
	return false
}

// adds condition to the SelectBuilder according to the AND/OR
func AddExpr(qb sq.SelectBuilder, e interface{}, AndOr string) sq.SelectBuilder {
	switch AndOr {
	case "":
		qb = qb.Where(e.(sq.Sqlizer))
	case "AND":
		qb = qb.Where(sq.And{e.(sq.Sqlizer)})
	case "OR":
		qb = qb.Where(sq.Or{e.(sq.Sqlizer)})
	}
	return qb
}

// parser
func Parse(query string, qb sq.SelectBuilder) (*sq.SelectBuilder, error) {

	qb = qb.PlaceholderFormat(sq.Dollar)

	forbiddenSQLWords := []string{"WHERE", "SELECT", "INSERT", "UPDATE", "DELETE", "VALUES", "FROM", "GROUP", "HAVING", "WINDOW", "ALL", "DISTINCT", "ORDER", "LIMIT", "OFFSET", "FETCH", "FOR", "SET"}
	for _, w := range forbiddenSQLWords {
		if strings.Contains(query, w) {
			return nil, errors.New(fmt.Sprintf("input string should have only WHERE part of the SQL query, but it has %v", w))
		}
	}

	// parsing the incoming string to the slice of Condition structs

	conditions := []Condition{}
	qStrings := strings.Fields(query)
	step := stepAndOr

	wn := 0
	cn := 0
	for wn < len(qStrings) {
		currWord := strings.ToUpper(qStrings[wn])
		nextWord := ""
		if wn+1 < len(qStrings) {
			nextWord = strings.ToUpper(qStrings[wn+1])
		}
		switch step {
		case stepAndOr:
			cond := new(Condition)
			if currWord == "AND" || currWord == "OR" {
				cond.AndOr = currWord
				wn++
			} else if len(conditions) > 0 {
				return nil, errors.New(fmt.Sprintf("next condition should start from AND/OR, got: %v", qStrings[wn]))
			}
			conditions = append(conditions, *cond)
			cn++
			if wn+1 > len(qStrings) {
				return nil, errors.New(fmt.Sprintf("no field after %v", conditions[cn-1].AndOr))
			}
			step = stepNot
		case stepNot:
			if currWord == "NOT" {
				conditions[cn-1].Not = true
				wn++
			} else {
				conditions[cn-1].Not = false
			}
			step = stepField
		case stepField:
			if !IsOperator(qStrings[wn]) && currWord != "NOT" && CheckIfField(qStrings[wn]) {
				conditions[cn-1].Field = qStrings[wn]
				wn++
			} else {
				return nil, errors.New(fmt.Sprintf("unexpected field: %v", qStrings[wn]))
			}

			if wn+1 > len(qStrings) {
				return nil, errors.New(fmt.Sprintf("no operator after field %v", conditions[cn-1].Field))
			}

			step = stepOperator
			if wn+1 < len(qStrings) {
				if nextWord == "AND" || nextWord == "OR" {
					step = stepAndOr
				} else {
					step = stepOperator
				}
			}
		case stepOperator:
			if currWord == "NOT" {
				if wn+1 < len(qStrings) {
					if IsDoubleOperator(currWord, nextWord) {
						conditions[cn-1].Operator = currWord + " " + nextWord
						wn++
					} else {
						return nil, errors.New(fmt.Sprintf("unexpected oprator: %v", qStrings[wn]))
					}
				}
			}

			if IsOperator(qStrings[wn]) {
				conditions[cn-1].Operator = qStrings[wn]
				wn++
			} else {
				return nil, errors.New(fmt.Sprintf("unexpected operator: %v", qStrings[wn]))
			}
			if wn+1 > len(qStrings) {
				return nil, errors.New(fmt.Sprintf("no value for field %v", conditions[cn-1].Field))
			}
			step = stepValue
		case stepValue:
			if !IsOperator(qStrings[wn]) && qStrings[wn] != "NOT" && qStrings[wn] != "" {
				conditions[cn-1].Value = qStrings[wn]
				wn++
			} else {
				return nil, errors.New(fmt.Sprintf("unexpected value: %v", qStrings[wn]))
			}
			step = stepAndOr
		}
	}

	// processing every struct, escaping, checking for correct names and types if the database is available,
	// adding conditions to the output squirrel.SelectBuilder

	for _, c := range conditions {

		// // testing if fields and value types of query are valid for the DB table
		// // removed until we do not know the DB and table

		// db, err := sql.Open("postgres", "postgres://postgres:pgpass@localhost:5432?sslmode=disable")
		// if err != nil {
		// 	panic(err)
		// }
		// defer db.Close()

		// err = CheckDataForDB(c.Field, c.Value, db, TableName)
		// if err != nil {
		// 	return nil, err
		// }

		if c.Operator != "" {
			if c.Operator == "=" {
				if !c.Not {
					e := sq.Eq{c.Field: c.Value}
					qb = AddExpr(qb, e, c.AndOr)
				} else {
					e := sq.NotEq{c.Field: c.Value}
					qb = AddExpr(qb, e, c.AndOr)
				}
			} else if c.Operator == ">" {
				if !c.Not {
					e := sq.Gt{c.Field: c.Value}
					qb = AddExpr(qb, e, c.AndOr)
				} else {
					e := sq.LtOrEq{c.Field: c.Value}
					qb = AddExpr(qb, e, c.AndOr)
				}
			} else if c.Operator == "<" {
				if !c.Not {
					e := sq.Lt{c.Field: c.Value}
					qb = AddExpr(qb, e, c.AndOr)
				} else {
					e := sq.GtOrEq{c.Field: c.Value}
					qb = AddExpr(qb, e, c.AndOr)
				}
			} else if c.Operator == ">=" {
				if !c.Not {
					e := sq.GtOrEq{c.Field: c.Value}
					qb = AddExpr(qb, e, c.AndOr)
				} else {
					e := sq.Lt{c.Field: c.Value}
					qb = AddExpr(qb, e, c.AndOr)
				}
			} else if c.Operator == "<=" {
				if !c.Not {
					e := sq.LtOrEq{c.Field: c.Value}
					qb = AddExpr(qb, e, c.AndOr)
				} else {
					e := sq.Gt{c.Field: c.Value}
					qb = AddExpr(qb, e, c.AndOr)
				}
			} else if c.Operator == "!=" || c.Operator == "<>" {
				if !c.Not {
					e := sq.NotEq{c.Field: c.Value}
					qb = AddExpr(qb, e, c.AndOr)
				} else {
					e := sq.Eq{c.Field: c.Value}
					qb = AddExpr(qb, e, c.AndOr)
				}
			} else if strings.ToUpper(c.Operator) == "LIKE" {
				if !c.Not {
					e := sq.Like{c.Field: c.Value}
					qb = AddExpr(qb, e, c.AndOr)
				} else {
					e := sq.NotLike{c.Field: c.Value}
					qb = AddExpr(qb, e, c.AndOr)
				}
			} else if strings.ToUpper(c.Operator) == "ILIKE" {
				if !c.Not {
					e := sq.ILike{c.Field: c.Value}
					qb = AddExpr(qb, e, c.AndOr)
				} else {
					e := sq.NotILike{c.Field: c.Value}
					qb = AddExpr(qb, e, c.AndOr)
				}
			} else if c.Operator == "~" || c.Operator == "~*" || c.Operator == "!~" || c.Operator == "!~*" {
				newQuery := ""
				for _, c := range conditions {
					if c.AndOr != "" {
						newQuery += c.AndOr + " "
					}
					if c.Not {
						newQuery += "NOT "
					}
					newQuery += c.Field + " "
					newQuery += c.Operator + " "
					newQuery += fmt.Sprint(c.Value) + " "
				}
				newQuery = strings.TrimSuffix(newQuery, " ")
				qb = qb.Where(newQuery)
				return &qb, nil
			}
		} else {
			if !c.Not {
				e := sq.Eq{c.Field: "TRUE"}
				qb = AddExpr(qb, e, c.AndOr)
			} else {
				e := sq.NotEq{c.Field: "TRUE"}
				qb = AddExpr(qb, e, c.AndOr)
			}
		}
	}

	return &qb, nil
}

func CheckDataForDB(colName string, value interface{}, db *sql.DB, tName string) error {

	isInTable := false
	var typ reflect.Kind
	if value != nil {
		typ = reflect.TypeOf(value).Kind()
	}
	columnNames, columnTypes, err := GetTableProps(db, tName)
	if err != nil {
		return err
	} else {
		for i := range columnNames {
			if columnNames[i] == colName {
				isInTable = true
				if value != nil {
					switch columnTypes[i] {
					case "int":
						if typ != reflect.Int {
							return errors.New(fmt.Sprintf("incorrect type for %v column, required int, got %v", colName, typ))
						}
					case "string":
						if typ != reflect.String {
							return errors.New(fmt.Sprintf("incorrect type for %v column, required string, got %v", colName, typ))
						}
					default:
						return errors.New(fmt.Sprintf("unexpected data type %v received from table %v, column %v", tName, columnTypes[i], typ))
					}
				}
			}
		}
	}
	if isInTable {
		return nil
	}
	return errors.New(fmt.Sprintf("There is no field %v in table %v", colName, tName))
}

// returns columnNames and columnTypes arrays from the database
func GetTableProps(db *sql.DB, tName string) ([]string, []string, error) {

	resColumns, err := db.Query("SELECT * FROM information_schema.columns WHERE table_name = $1", tName)
	if err != nil {
		return nil, nil, err
	}

	columns := []interface{}{}
	for i := 0; i < 44; i++ {
		columns = append(columns, new(sql.NullString))
	}

	var columnNames []string
	var columnTypes []string

	for resColumns.Next() {
		err = resColumns.Scan(columns...)
		if err != nil {
			return nil, nil, err
		}

		columnNames = append(columnNames, *&columns[3].(*sql.NullString).String)
		switch *&columns[7].(*sql.NullString).String {
		case "character varying", "text", "varchar", "char", "character":
			columnTypes = append(columnTypes, "string")
		case "integer":
			columnTypes = append(columnTypes, "int")
		default:
			columnTypes = append(columnTypes, *&columns[7].(*sql.NullString).String)
		}
	}
	resColumns.Close()
	return columnNames, columnTypes, nil
}

func CheckIfField(field string) bool {
	return r.MatchString(field)
}
