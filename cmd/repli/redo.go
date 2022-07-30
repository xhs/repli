package main

type RedoConfig struct {
	RedoFile          string `short:"F" long:"redo-file" description:"Redo replication from error log" value-name:"<FILENAME>"`
	DeleteMissingKeys bool   `long:"delete-missing-keys" description:"Delete keys missing in source from target" value-name:"<BOOL>"`
}

var redoCommand RedoConfig

func (r *RedoConfig) Execute(args []string) error {
	command = "REDO"
	return nil
}

func init() {
	parser.AddCommand("redo", "", "Redo replication from error log", &redoCommand)
}
