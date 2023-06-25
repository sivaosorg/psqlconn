package postgresconn

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/jmoiron/sqlx"

	_ "github.com/lib/pq"
)

type PostgresService interface {
	GetPIDConn() (int, error)
	Database() string
	GetAllTables() ([]string, error)
	GetAllFunctions() ([]string, error)
	GetAllProcedures() ([]string, error)
	GetFunctionDetails(function string) ([]PostgresFunctionDetail, error)
	GetFunctionReturnType(function string) (string, error)
	BuildFunction(function string) (string, error)
	ShowFunctionContent(function string) (string, error)
	ShowProcedureContent(procedure string) (string, error)
	ExplainAnalysis(query string) (string, error)
	ExplainAnalysisFromFile(filename string) (string, error)
}

type postgresServiceImpl struct {
	dbConn *sqlx.DB
}

func NewPostgresService(dbConn *sqlx.DB) PostgresService {
	p := &postgresServiceImpl{
		dbConn: dbConn,
	}
	return p
}

func (p *postgresServiceImpl) GetPIDConn() (int, error) {
	var pid int
	err := p.dbConn.QueryRow("SELECT pg_backend_pid() AS pid").Scan(&pid)
	if err != nil {
		return 0, err
	}
	return pid, nil
}

func (p *postgresServiceImpl) GetAllTables() ([]string, error) {
	var tableNames []string
	err := p.dbConn.Select(&tableNames, "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
	if err != nil {
		return nil, err
	}
	return tableNames, nil
}

func (p *postgresServiceImpl) GetAllFunctions() ([]string, error) {
	var functions []string
	err := p.dbConn.Select(&functions, "SELECT routine_name FROM information_schema.routines WHERE routine_catalog = $1 AND routine_schema = 'public' AND routine_type = 'FUNCTION'", p.Database())
	if err != nil {
		return nil, err
	}
	return functions, nil
}

func (p *postgresServiceImpl) Database() string {
	var database string
	err := p.dbConn.Get(&database, "SELECT current_database()")
	if err != nil {
		panic(err)
	}
	return database
}

func (p *postgresServiceImpl) GetAllProcedures() ([]string, error) {
	var procedures []string
	err := p.dbConn.Select(&procedures, "SELECT routine_name FROM information_schema.routines WHERE routine_catalog = $1 AND routine_schema = 'public' AND routine_type = 'PROCEDURE'", p.Database())
	if err != nil {
		return nil, err
	}
	return procedures, nil
}

func (p *postgresServiceImpl) GetFunctionDetails(function string) ([]PostgresFunctionDetail, error) {
	var functionDetails []PostgresFunctionDetail
	err := p.dbConn.Select(&functionDetails, `
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

func (p *postgresServiceImpl) GetFunctionReturnType(function string) (string, error) {
	var returnType string
	err := p.dbConn.QueryRow("SELECT pg_get_function_result(oid) FROM pg_proc WHERE proname = $1", function).Scan(&returnType)
	if err != nil {
		return "", err
	}
	return returnType, nil
}

func (p *postgresServiceImpl) BuildFunction(function string) (string, error) {
	functionDetails, err := p.GetFunctionDetails(function)
	if err != nil {
		return "", err
	}
	returnType, err := p.GetFunctionReturnType(function)
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

func (ps *postgresServiceImpl) ShowFunctionContent(function string) (string, error) {
	var functionContent string
	err := ps.dbConn.QueryRow("SELECT pg_get_functiondef($1::regproc)", function).Scan(&functionContent)
	if err != nil {
		return "", err
	}
	return functionContent, nil
}

func (p *postgresServiceImpl) ShowProcedureContent(procedure string) (string, error) {
	var procedureContent string
	err := p.dbConn.QueryRow("SELECT pg_get_functiondef($1::regproc)", procedure).Scan(&procedureContent)
	if err != nil {
		return "", err
	}
	return procedureContent, nil
}

func (p *postgresServiceImpl) ExplainAnalysis(query string) (string, error) {
	rows, err := p.dbConn.Query(fmt.Sprintf("EXPLAIN ANALYZE %v", query))
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

func (p *postgresServiceImpl) ExplainAnalysisFromFile(filename string) (string, error) {
	bytes, err := ioutil.ReadFile(filepath.Clean(filename))
	if err != nil {
		return "", err
	}
	query := string(bytes)
	return p.ExplainAnalysis(query)
}
