## repli

Simple live replication tool for Redis

### Usage

```
Usage:
  repli [OPTIONS] <redo | run> [Command-OPTIONS]

Application Options:
  -f, --from=[<IP>]:<PORT>                 Endpoint of source Redis instance
      --read-only                          Send READONLY command before replicating
  -t, --to=[<IP>]:<PORT>                   Endpoint of target Redis instance/cluster
  -d, --database=<INT>                     Redis database to replicate (default: 0)
  -c, --cluster                            Replicate to Redis cluster
      --read-timeout=<SECONDS>             Read timeout in seconds (default: 5)
      --write-timeout=<SECONDS>            Write timeout in seconds (default: 5)
      --max-retries=<INT>                  Maximum retries of connecting before giving up (default: 10)
      --keyspace-pattern=<GLOB-PATTERN>    Redis key pattern to match (default: *)
      --skip-pattern=<REGEXP>              Key patterns to skip, can be specified multiple times

Help Options:
  -h, --help                               Show this help message

Available commands:
  redo
  run

[redo command options]
      -F, --redo-file=<FILENAME>           redo replication from error log
          --delete-missing-keys            delete keys missing in source from target

[run command options]
      -n, --replicator-number=<INT>               Number of concurrent replicators (default: 1)
      -b, --write-batch-size=<INT>                Batch size of Redis writing pipeline (default: 50)
      -l, --write-batch-latency=<MILLISECONDS>    Maximum milliseconds before a batch is written (default: 100)
      -B, --read-batch-size=<INT>                 Batch size of Redis reading pipeline (default: 30)
      -L, --read-batch-latency=<MILLISECONDS>     Maximum milliseconds before a batch is read (default: 50)
      -s, --event-queue-size=<INT>                Size of keyspace event queue (default: 10000)
      -T, --min-ttl=<SECONDS>                     Minimum TTL in seconds, keys with remaining TTL less than this value will be ignored (default: 3)
      -i, --report-interval=<SECONDS>             Interval seconds to log status report (default: 5)
```
