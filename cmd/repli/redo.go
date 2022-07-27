package main

type redoCommand struct {
	RedoFile string `short:"F" long:"redo-file" description:"redo replication from error log" value-name:"<FILENAME>"`
}

var redoConfig redoCommand

func (r *redoCommand) Execute(args []string) error {
	mode = "REDO"
	return nil
}

func init() {
	parser.AddCommand("redo", "", "Redo replication from error log", &redoConfig)
}
