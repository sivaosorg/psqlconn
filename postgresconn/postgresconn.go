package postgresconn

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/sivaosorg/govm/callback"
	"github.com/sivaosorg/govm/common"
	"github.com/sivaosorg/govm/dbx"
	"github.com/sivaosorg/govm/logger"
	"github.com/sivaosorg/govm/postgres"

	_ "github.com/lib/pq"
)

var (
	instance *sqlx.DB
	_logger  = logger.NewLogger()
)

func NewClient(config postgres.PostgresConfig) (*sqlx.DB, dbx.Dbx) {
	s := dbx.NewDbx().SetDatabase(config.Database)
	if !config.IsEnabled {
		s.SetConnected(false).
			SetMessage("Postgres unavailable").
			SetError(fmt.Errorf(s.Message))
		return &sqlx.DB{}, *s
	}
	if instance != nil {
		s.SetConnected(true)
		return instance, *s
	}
	stringConn := fmt.Sprintf("host=%s port=%d user=%s dbname=%s password=%s sslmode=%s",
		config.Host, config.Port, config.Username, config.Database, config.Password, config.SSLMode)
	if config.DebugMode {
		_logger.Info(fmt.Sprintf("Postgres client connection:: %s", stringConn))
	}
	client, err := sqlx.Open(common.EntryKeyPostgres, stringConn)
	if err != nil {
		s.SetError(err).SetConnected(false).SetMessage(err.Error())
		return nil, *s
	}
	err = client.Ping()
	if err != nil {
		s.SetError(err).SetConnected(false).SetMessage(err.Error())
		return nil, *s
	}
	if config.DebugMode {
		_logger.Info(fmt.Sprintf("Connected successfully to postgres database %s:%d/%s", config.Host, config.Port, config.Database))
	}
	client.SetMaxIdleConns(config.MaxIdleConn)
	client.SetMaxOpenConns(config.MaxOpenConn)
	instance = client
	if config.DebugMode {
		callback.MeasureTime(func() {
			pid, err := GetPostgresPIDConn(instance)
			if err == nil {
				_logger.Info("Postgres client connection PID:: %d", pid)
			}
		})
	}
	s.SetConnected(true).SetMessage("Connection established")
	return instance, *s
}

func GetPostgresPIDConn(db *sqlx.DB) (int, error) {
	s := NewPostgresService(db)
	return s.GetPIDConn()
}
