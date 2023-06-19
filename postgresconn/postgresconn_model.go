package postgresconn

type PostgresFunctionDetail struct {
	RoutineName   string `db:"routine_name" json:"routine_name"`
	DataType      string `db:"data_type" json:"data_type"`
	ParameterName string `db:"parameter_name" json:"parameter_name"`
	ParameterMode string `db:"parameter_mode" json:"parameter_mode"`
}
