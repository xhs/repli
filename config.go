package repli

type CommonConfig struct {
	SourceEndpoint  string   `short:"f" long:"from" description:"Endpoint of source Redis instance" value-name:"[<IP>]:<PORT>" required:"true"`
	ReadOnly        bool     `long:"read-only" description:"Send READONLY command before replicating" value-name:"<BOOL>"`
	TargetEndpoint  string   `short:"t" long:"to" description:"Endpoint of target Redis instance/cluster" value-name:"[<IP>]:<PORT>" required:"true"`
	RedisDatabase   int      `short:"d" long:"database" description:"Redis database to replicate" value-name:"<INT>" default:"0"`
	ClusterMode     bool     `short:"c" long:"cluster" description:"Replicate to Redis cluster" value-name:"<BOOL>"`
	ReadTimeout     int      `long:"read-timeout" description:"Read timeout in seconds" value-name:"<SECONDS>" default:"5"`
	WriteTimeout    int      `long:"write-timeout" description:"Write timeout in seconds" value-name:"<SECONDS>" default:"5"`
	MaxRetries      int      `long:"max-retries" description:"Maximum retries of connecting before giving up" value-name:"<INT>" default:"10"`
	SkipKeyPatterns []string `long:"skip-pattern" description:"Key patterns to skip" value-name:"<PATTERN>"`
}
