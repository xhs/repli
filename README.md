## repli

```
Usage:
  repli [OPTIONS]

Application Options:
  -f, --from=[<IP>]:<PORT>                    Endpoint of source Redis instance
  -t, --to=[<IP>]:<PORT>                      Endpoint of target Redis instance/cluster
  -d, --database=<INT>                        Redis database to replicate (default: 0)
  -c, --cluster                               Replicate to Redis cluster
  -n, --replicator-number=<INT>               Number of concurrent replicators (default: 1)
  -b, --write-batch-size=<INT>                Batch size of Redis writing pipeline (default: 50)
  -l, --write-batch-latency=<MILLISECONDS>    Maximum milliseconds before a batch is written (default: 100)
  -B, --read-batch-size=<INT>                 Batch size of Redis reading pipeline (default: 30)
  -L, --read-batch-latency=<MILLISECONDS>     Maximum milliseconds before a batch is read (default: 50)
  -s, --event-queue-size=<INT>                Size of keyspace event queue (default: 10000)
  -T, --min-ttl=<SECONDS>                     Minimum TTL in seconds, keys with remaining TTL less than this value will be ignored (default: 3)
  -i, --report-interval=<SECONDS>             Interval seconds to log status report (default: 5)

Help Options:
  -h, --help                                  Show this help message
```
