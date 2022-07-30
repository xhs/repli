package main

type RunConfig struct {
	ReplicatorNumber  int    `short:"n" long:"replicator-number" description:"Number of concurrent replicators" value-name:"<INT>" default:"1"`
	WriteBatchSize    int    `short:"b" long:"write-batch-size" description:"Batch size of Redis writing pipeline" value-name:"<INT>" default:"50"`
	WriteBatchLatency int    `short:"l" long:"write-batch-latency" description:"Maximum milliseconds before a batch is written" value-name:"<MILLISECONDS>" default:"100"`
	ReadBatchSize     int    `short:"B" long:"read-batch-size" description:"Batch size of Redis reading pipeline" value-name:"<INT>" default:"30"`
	ReadBatchLatency  int    `short:"L" long:"read-batch-latency" description:"Maximum milliseconds before a batch is read" value-name:"<MILLISECONDS>" default:"50"`
	EventQueueSize    int    `short:"s" long:"event-queue-size" description:"Size of keyspace event queue" value-name:"<INT>" default:"10000"`
	MinTTL            int    `short:"T" long:"min-ttl" description:"Minimum TTL in seconds, keys with remaining TTL less than this value will be ignored" value-name:"<SECONDS>" default:"3"`
	ReportInterval    int    `short:"i" long:"report-interval" description:"Interval seconds to log status report" value-name:"<SECONDS>" default:"5"`
	Mode              string `short:"m" long:"mode" description:"Replication mode" choice:"full" choice:"snapshot" choice:"live" default:"full"`
}

var runCommand RunConfig

func (r *RunConfig) Execute(args []string) error {
	command = "RUN"
	return nil
}

func init() {
	parser.AddCommand("run", "", "Start replication", &runCommand)
}
