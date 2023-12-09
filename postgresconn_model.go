package postgresconn

import (
	"github.com/jmoiron/sqlx"
	"github.com/sivaosorg/govm/dbx"
	"github.com/sivaosorg/govm/postgres"
	"gopkg.in/guregu/null.v3"
)

type Postgres struct {
	conn   *sqlx.DB                `json:"-"`
	Config postgres.PostgresConfig `json:"config,omitempty"`
	State  dbx.Dbx                 `json:"state,omitempty"`
}

type IFunctionDescriptor struct {
	RoutineName   string `db:"routine_name" json:"routine_name,omitempty"`
	DataType      string `db:"data_type" json:"type,omitempty"`
	ParameterName string `db:"parameter_name" json:"param_name,omitempty"`
	ParameterMode string `db:"parameter_mode" json:"param_mode,omitempty"`
}

type ITableDescriptor struct {
	Name       string `json:"name,omitempty" db:"c_name"`
	Type       string `json:"type,omitempty" db:"type"`
	Descriptor string `json:"descriptor,omitempty" db:"descriptor"`
}

type ITableInfo struct {
	Column    string   `json:"column" db:"column_name"`
	Type      string   `json:"type" db:"data_type"`
	MaxLength null.Int `json:"max_length" db:"character_maximum_length"`
}
