package postgresconn

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
