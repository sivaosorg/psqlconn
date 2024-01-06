package example

import (
	"testing"

	"github.com/sivaosorg/govm/dbx"
	"github.com/sivaosorg/govm/logger"
	"github.com/sivaosorg/govm/postgres"
	"github.com/sivaosorg/psqlconn"
)

func createConn() (*psqlconn.Postgres, dbx.Dbx) {
	return psqlconn.NewClient(*postgres.GetPostgresConfigSample().
		SetDebugMode(false).
		SetEnabled(true).
		SetPort(6666).
		SetUsername("tms_admin").
		SetDatabase("db").
		SetPassword("Usfy3siOO%fX"))
}

func TestConn(t *testing.T) {
	_, s := createConn()
	logger.Infof("Postgres connection status: %v", s)
}

func TestServiceTables(t *testing.T) {
	p, _ := createConn()
	svc := psqlconn.NewPostgresService(p)
	tables, err := svc.Tables()
	if err != nil {
		logger.Errorf("Fetching all tables got an error", err)
		return
	}
	logger.Infof("All tables: %v", tables)
}

func TestServiceFunctions(t *testing.T) {
	p, _ := createConn()
	svc := psqlconn.NewPostgresService(p)
	functions, err := svc.FunctionsDescriptor()
	if err != nil {
		logger.Errorf("Fetching all functions got an error", err)
		return
	}
	logger.Infof("All functions: %v", functions)
}

func TestServiceProduces(t *testing.T) {
	p, _ := createConn()
	svc := psqlconn.NewPostgresService(p)
	functions, err := svc.ProceduresDescriptor()
	if err != nil {
		logger.Errorf("Fetching all procedures got an error", err)
		return
	}
	logger.Infof("All procedures: %v", functions)
}

func TestServiceFunctionDescriptor(t *testing.T) {
	p, _ := createConn()
	svc := psqlconn.NewPostgresService(p)
	tables, err := svc.FunctionDescriptor("get_activelead_v7")
	if err != nil {
		logger.Errorf("Fetching function descriptor got an error", err)
		return
	}
	logger.Infof("Function descriptor: %v", tables)
}

func TestServiceFunctionTypeDescriptor(t *testing.T) {
	p, _ := createConn()
	svc := psqlconn.NewPostgresService(p)
	tables, err := svc.FunctionDDescriptor("get_activelead_v7")
	if err != nil {
		logger.Errorf("Fetching function descriptor got an error", err)
		return
	}
	logger.Infof("Function descriptor: %v", tables)
}

func TestServiceTableTypeDescriptor(t *testing.T) {
	p, _ := createConn()
	svc := psqlconn.NewPostgresService(p)
	desc, err := svc.TableDescriptor("or_user")
	if err != nil {
		logger.Errorf("Fetching table descriptor got an error", err)
		return
	}
	logger.Infof("Table descriptor: %v", desc)
}

func TestServiceTableDescriptor(t *testing.T) {
	p, _ := createConn()
	svc := psqlconn.NewPostgresService(p)
	desc, err := svc.TableInfo("or_user")
	if err != nil {
		logger.Errorf("Fetching table info got an error", err)
		return
	}
	logger.Infof("Table info: %v", desc)
}
