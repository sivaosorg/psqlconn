package postgresconn

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	_ "github.com/lib/pq"
)

type PostgresService interface {
	Pid() (int, error)
	Database() string
	Tables() ([]string, error)
	FunctionsDescriptor() ([]string, error)
	ProceduresDescriptor() ([]string, error)
	FunctionDDescriptor(function string) ([]IFunctionDescriptor, error)
	FunctionReturnType(function string) (string, error)
	AddFunction(function string) (string, error)
	FunctionDescriptor(function string) (string, error)
	ProcedureDescriptor(procedure string) (string, error)
	ExplainAnalysis(query string) (string, error)
	ExplainAnalysisFile(filename string) (string, error)
	ExecuteBatch(statements []string) error
	ExecuteBatchWithTransaction(statements []string) error
	TableDescriptor(table string) ([]ITableDescriptor, error)
	TableInfo(table string) ([]ITableInfo, error)
}

type postgresServiceImpl struct {
	dbConn *Postgres
}

func NewPostgresService(dbConn *Postgres) PostgresService {
	p := &postgresServiceImpl{
		dbConn: dbConn,
	}
	return p
}

func (p *postgresServiceImpl) Pid() (int, error) {
	var pid int
	err := p.dbConn.conn.QueryRow("SELECT pg_backend_pid() AS pid").Scan(&pid)
	if err != nil {
		return 0, err
	}
	return pid, nil
}

func (p *postgresServiceImpl) Tables() ([]string, error) {
	var tableNames []string
	err := p.dbConn.conn.Select(&tableNames, "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
	if err != nil {
		return nil, err
	}
	return tableNames, nil
}

func (p *postgresServiceImpl) FunctionsDescriptor() ([]string, error) {
	var functions []string
	err := p.dbConn.conn.Select(&functions, "SELECT routine_name FROM information_schema.routines WHERE routine_catalog = $1 AND routine_schema = 'public' AND routine_type = 'FUNCTION'", p.Database())
	if err != nil {
		return nil, err
	}
	return functions, nil
}

func (p *postgresServiceImpl) Database() string {
	var database string
	err := p.dbConn.conn.Get(&database, "SELECT current_database()")
	if err != nil {
		panic(err)
	}
	return database
}

func (p *postgresServiceImpl) ProceduresDescriptor() ([]string, error) {
	var procedures []string
	err := p.dbConn.conn.Select(&procedures, "SELECT routine_name FROM information_schema.routines WHERE routine_catalog = $1 AND routine_schema = 'public' AND routine_type = 'PROCEDURE'", p.Database())
	if err != nil {
		return nil, err
	}
	return procedures, nil
}

func (p *postgresServiceImpl) FunctionDDescriptor(function string) ([]IFunctionDescriptor, error) {
	var functionDetails []IFunctionDescriptor
	err := p.dbConn.conn.Select(&functionDetails, `
	SELECT 
		r.routine_name, 
		p.data_type, 
		p.parameter_name, 
		p.parameter_mode 
	FROM information_schema.routines r 
	JOIN information_schema.parameters p 
		ON r.specific_name = p.specific_name 
	WHERE r.routine_catalog = $1 
		AND r.routine_schema = 'public' 
		AND r.routine_name = $2
        `, p.Database(), function)
	if err != nil {
		return nil, err
	}
	return functionDetails, nil
}

func (p *postgresServiceImpl) FunctionReturnType(function string) (string, error) {
	var returnType string
	err := p.dbConn.conn.QueryRow("SELECT pg_get_function_result(oid) FROM pg_proc WHERE proname = $1", function).Scan(&returnType)
	if err != nil {
		return "", err
	}
	return returnType, nil
}

func (p *postgresServiceImpl) AddFunction(function string) (string, error) {
	functionDetails, err := p.FunctionDDescriptor(function)
	if err != nil {
		return "", err
	}
	returnType, err := p.FunctionReturnType(function)
	if err != nil {
		return "", err
	}
	var builder strings.Builder
	builder.WriteString("CREATE OR REPLACE FUNCTION " + function + "(")
	for i, detail := range functionDetails {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(detail.ParameterName + " " + detail.DataType)
		if detail.ParameterMode != "IN" {
			builder.WriteString(" " + detail.ParameterMode)
		}
	}
	builder.WriteString(") RETURNS " + returnType + " AS $$\n")
	builder.WriteString("BEGIN\n\n")
	builder.WriteString("\n")
	builder.WriteString("\nEND;\n$$ LANGUAGE plpgsql;")
	return builder.String(), nil
}

func (ps *postgresServiceImpl) FunctionDescriptor(function string) (string, error) {
	var functionContent string
	err := ps.dbConn.conn.QueryRow("SELECT pg_get_functiondef($1::regproc)", function).Scan(&functionContent)
	if err != nil {
		return "", err
	}
	return functionContent, nil
}

func (p *postgresServiceImpl) ProcedureDescriptor(procedure string) (string, error) {
	var procedureContent string
	err := p.dbConn.conn.QueryRow("SELECT pg_get_functiondef($1::regproc)", procedure).Scan(&procedureContent)
	if err != nil {
		return "", err
	}
	return procedureContent, nil
}

func (p *postgresServiceImpl) ExplainAnalysis(query string) (string, error) {
	rows, err := p.dbConn.conn.Query(fmt.Sprintf("EXPLAIN ANALYZE %v", query))
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var explain strings.Builder
	for rows.Next() {
		var line string
		if err = rows.Scan(&line); err != nil {
			return "", err
		}
		explain.WriteString(line)
		explain.WriteString("\n")
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	return explain.String(), nil
}

func (p *postgresServiceImpl) ExplainAnalysisFile(filename string) (string, error) {
	bytes, err := ioutil.ReadFile(filepath.Clean(filename))
	if err != nil {
		return "", err
	}
	query := string(bytes)
	return p.ExplainAnalysis(query)
}

func (p *postgresServiceImpl) ExecuteBatch(statements []string) error {
	if len(statements) == 0 {
		return fmt.Errorf("missing statements")
	}
	tx, err := p.dbConn.conn.Beginx()
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		} else if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()
	for _, statement := range statements {
		_, err := tx.Exec(statement)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *postgresServiceImpl) ExecuteBatchWithTransaction(statements []string) error {
	tx, err := p.dbConn.conn.Beginx()
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		} else if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()
	err = p.ExecuteBatch(statements)
	if err != nil {
		return err
	}
	return nil
}

func (p *postgresServiceImpl) TableDescriptor(table string) ([]ITableDescriptor, error) {
	s := `
	SELECT conname AS c_name, 'Primary Key' AS type, '' as descriptor
	FROM pg_constraint
	WHERE conrelid = regclass($1)
	  AND confrelid = 0
	  AND contype = 'p'
	UNION
	SELECT conname AS c_name, 'Unique Key' AS type, '' as descriptor
	FROM pg_constraint
	WHERE conrelid = regclass($1)
	  AND confrelid = 0
	  AND contype = 'u'
	UNION
	SELECT indexname AS c_name, 'Index' AS type, indexdef as descriptor
	FROM pg_indexes
	WHERE tablename = $1;
	`
	rows, err := p.dbConn.conn.Query(s, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []ITableDescriptor
	for rows.Next() {
		var m ITableDescriptor
		if err := rows.Scan(&m.Name, &m.Type, &m.Descriptor); err != nil {
			return nil, err
		}
		results = append(results, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func (p *postgresServiceImpl) TableInfo(table string) ([]ITableInfo, error) {
	s := `
	SELECT
		column_name,
		data_type,
		character_maximum_length
	FROM
		information_schema.columns
	WHERE
		table_name = $1;
	`
	rows, err := p.dbConn.conn.Query(s, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []ITableInfo
	for rows.Next() {
		var m ITableInfo
		if err := rows.Scan(&m.Column, &m.Type, &m.MaxLength); err != nil {
			return nil, err
		}
		results = append(results, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}
