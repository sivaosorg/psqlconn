package postgresconn

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/sivaosorg/govm/callback"
	"github.com/sivaosorg/govm/common"
	"github.com/sivaosorg/govm/dbx"
	"github.com/sivaosorg/govm/logger"
	"github.com/sivaosorg/govm/postgres"
	"github.com/sivaosorg/govm/utils"

	_ "github.com/lib/pq"
)

var (
	instance *Postgres
	_logger  = logger.NewLogger()
)

func NewPostgres() *Postgres {
	p := &Postgres{}
	return p
}

func (p *Postgres) SetConn(value *sqlx.DB) *Postgres {
	p.conn = value
	return p
}

func (p *Postgres) SetConfig(value postgres.PostgresConfig) *Postgres {
	p.Config = value
	return p
}

func (p *Postgres) SetState(value dbx.Dbx) *Postgres {
	p.State = value
	return p
}

func (p *Postgres) Close() error {
	return p.conn.Close()
}

func (p *Postgres) Json() string {
	return utils.ToJson(p)
}

func (p *Postgres) GetConn() *sqlx.DB {
	return p.conn
}

func NewClient(config postgres.PostgresConfig) (*Postgres, dbx.Dbx) {
	s := dbx.NewDbx().SetDatabase(config.Database)
	if !config.IsEnabled {
		s.SetConnected(false).
			SetMessage("Postgres unavailable").
			SetError(fmt.Errorf(s.Message))
		return &Postgres{}, *s
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
	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()
	err = client.PingContext(ctx)
	if err != nil {
		s.SetError(err).SetConnected(false).SetMessage(err.Error())
		return nil, *s
	}
	if config.DebugMode {
		_logger.Info(fmt.Sprintf("Connected successfully to postgres database %s:%d/%s", config.Host, config.Port, config.Database))
	}
	client.SetMaxIdleConns(config.MaxIdleConn)
	client.SetMaxOpenConns(config.MaxOpenConn)
	instance = NewPostgres().SetConn(client)
	s.SetConnected(true).SetMessage("Connection established").SetNewInstance(true)
	if config.DebugMode {
		callback.MeasureTime(func() {
			pid, err := GetPidConn(instance)
			if err == nil {
				_logger.Info("Postgres client connection PID:: %d", pid)
			}
			s.SetPid(pid)
		})
	}
	instance.SetState(*s)
	return instance, *s
}

func GetPidConn(db *Postgres) (int, error) {
	s := NewPostgresService(db)
	return s.Pid()
}
