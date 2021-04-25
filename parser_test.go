package main

import (
	"database/sql"
	"errors"
	"testing"

	sq "github.com/Masterminds/squirrel"
)

type Case struct {
	query  string
	expSQL string
	expErr error
}

type CaseDB struct {
	field  string
	value  interface{}
	expErr error
}

var TableName = "cats"

func runCases(t *testing.T, cases []Case) {

	testQb := sq.Select("*").From(TableName)

	for i, c := range cases {
		qb, err := Parse(c.query, testQb)
		if err == nil {

			sql, _, errSql := qb.ToSql()
			if errSql != nil {
				t.Fatalf("ToSql error: %v", errSql.Error())
			} else {
				if sql != c.expSQL {
					t.Fatalf("Unexpected result for case %v. Expected: %v, got: %v", i, c.expSQL, sql)
				}
			}
		}
		if err != nil && c.expErr != nil {
			if err.Error() != c.expErr.Error() {
				t.Fatalf("Unexpected error for case %v. Expected: %v, got: %v", i, c.expErr, err)
			}
		} else if err == nil && c.expErr == nil {
			continue
		} else {
			t.Fatalf("Unexpected error for case %v. Expected: %v, got: %v", i, c.expErr, err)
		}
	}
}

func runCasesDB(t *testing.T, cases []CaseDB, db *sql.DB, table string) {

	for i, c := range cases {

		err := CheckDataForDB(c.field, c.value, db, TableName)
		if err != nil && c.expErr != nil {
			if err.Error() != c.expErr.Error() {
				t.Fatalf("Unexpected error for case %v. Expected: %v, got: %v", i, c.expErr, err)
			}
		} else if err == nil && c.expErr == nil {
			continue
		} else {
			t.Fatalf("Unexpected error for case %v. Expected: %v, got: %v", i, c.expErr, err)
		}
	}
}

func TestParser(t *testing.T) {
	cases := []Case{
		{
			query:  "Foo.Bar.X = 'hello'",
			expSQL: "SELECT * FROM " + TableName + " WHERE Foo.Bar.X = $1",
			expErr: nil,
		},
		{
			query:  "Bar.Alpha = 7",
			expSQL: "SELECT * FROM " + TableName + " WHERE Bar.Alpha = $1",
			expErr: nil,
		},
		{
			query:  "Foo.Bar.Beta > 21 AND Alpha.Bar != 'hello'",
			expSQL: "SELECT * FROM " + TableName + " WHERE Foo.Bar.Beta > $1 AND (Alpha.Bar <> $2)",
			expErr: nil,
		},
		{
			query:  "Alice.IsActive AND Bob.LastHash = 'ab5534b'",
			expSQL: "SELECT * FROM " + TableName + " WHERE Alice.IsActive = $1 AND (Bob.LastHash = $2)",
			expErr: nil,
		},
		{
			query:  "Alice.Name ~ 'A.*` OR Bob.LastName !~ 'Bill.*`",
			expSQL: "SELECT * FROM cats WHERE Alice.Name ~ 'A.*` OR Bob.LastName !~ 'Bill.*`",
			expErr: nil,
		},
		{
			query:  "NOT Alice.IsActive AND Bob.LastHash = 'ab5534b'",
			expSQL: "SELECT * FROM " + TableName + " WHERE Alice.IsActive <> $1 AND (Bob.LastHash = $2)",
			expErr: nil,
		},
		{
			query:  "Alice.IsActive AND NOT Bob.LastHash = 'ab5534b'",
			expSQL: "SELECT * FROM " + TableName + " WHERE Alice.IsActive = $1 AND (Bob.LastHash <> $2)",
			expErr: nil,
		},
		{
			query:  "Foo.Bar.X 'hello'",
			expSQL: "",
			expErr: errors.New("unexpected operator: 'hello'"),
		},
		{
			query:  "Foo.Bar.Beta > 21 Alpha.Bar != 'hello'",
			expSQL: "",
			expErr: errors.New("next condition should start from AND/OR, got: Alpha.Bar"),
		},
		{
			query:  "Foo.Bar.Beta > 21 AND Alpha.Bar != ",
			expSQL: "",
			expErr: errors.New("no value for field Alpha.Bar"),
		},
		{
			query:  "Foo.Bar.Beta > 21 AND ",
			expSQL: "",
			expErr: errors.New("no field after AND"),
		},
	}

	runCases(t, cases)
}

func TestParserDB(t *testing.T) {
	casesCheckDBFields := []CaseDB{
		{
			field:  "id",
			value:  1,
			expErr: nil,
		},
		{
			field:  "name",
			value:  "Dar",
			expErr: nil,
		},
		{
			field:  "id",
			value:  "Dar",
			expErr: errors.New("incorrect type for id column, required int, got string"),
		},
		{
			field:  "name",
			value:  1,
			expErr: errors.New("incorrect type for name column, required string, got int"),
		},
		{
			field:  "idddd",
			value:  1,
			expErr: errors.New("There is no field idddd in table cats"),
		},
		{
			field:  "Foo.Bar.Alpha",
			value:  "boris",
			expErr: nil,
		},
	}

	db, err := sql.Open("postgres", "postgres://postgres:pgpass@localhost:5432?sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	runCasesDB(t, casesCheckDBFields, db, TableName)
}
