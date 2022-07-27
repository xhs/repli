## repli

Simple live replication tool for Redis

### Usage

```
Usage:
  repli [OPTIONS]

Application Options:
  -f, --from=[<IP>]:<PORT>                    Endpoint of source Redis instance
      --read-only                             Send readonly command before replicating
  -t, --to=[<IP>]:<PORT>                      Endpoint of target Redis instance/cluster
  -d, --database=<INT>                        Redis database to replicate (default: 0)
  -c, --cluster                               Replicate to Redis cluster
  -n, --replicator-number=<INT>               Number of concurrent replicators (default: 1)
  -b, --write-batch-size=<INT>                Batch size of Redis writing pipeline (default: 50)
  -l, --write-batch-latency=<MILLISECONDS>    Maximum milliseconds before a batch is written (default: 100)
  -B, --read-batch-size=<INT>                 Batch size of Redis reading pipeline (default: 30)
  -L, --read-batch-latency=<MILLISECONDS>     Maximum milliseconds before a batch is read (default: 50)
  -s, --event-queue-size=<INT>                Size of keyspace event queue (default: 10000)
      --read-timeout=<SECONDS>                Read timeout in seconds (default: 5)
      --write-timeout=<SECONDS>               Write timeout in seconds (default: 5)
      --max-retries=<INT>                     Maximum number of retries before giving up (default: 10)
  -T, --min-ttl=<SECONDS>                     Minimum TTL in seconds, keys with remaining TTL less than this value will be ignored (default: 3)
  -i, --report-interval=<SECONDS>             Interval seconds to log status report (default: 5)

Help Options:
  -h, --help                                  Show this help message
```

### Running in action

```
./repli -f 172.20.33.50:13511 -t 10.218.75.68:13502 -c -n 2 -s 10000000 -b 50 -l 100 -B 30 -L 50

...
INFO[1350] status report                                 eventsArrived=369 eventsProcessed=369 keysQueried=367 keysReplicated=360 queued=0/10000000 replicators=2 source="172.20.33.50:13511" target="10.218.75.68:13502"
INFO[1355] status report                                 eventsArrived=1202 eventsProcessed=1202 keysQueried=1150 keysReplicated=1103 queued=0/10000000 replicators=2 source="172.20.33.50:13511" target="10.218.75.68:13502"
INFO[1360] status report                                 eventsArrived=2690 eventsProcessed=2690 keysQueried=2670 keysReplicated=2690 queued=0/10000000 replicators=2 source="172.20.33.50:13511" target="10.218.75.68:13502"
INFO[1365] status report                                 eventsArrived=3378 eventsProcessed=3378 keysQueried=3371 keysReplicated=3390 queued=0/10000000 replicators=2 source="172.20.33.50:13511" target="10.218.75.68:13502"
INFO[1370] status report                                 eventsArrived=2530 eventsProcessed=2465 keysQueried=2438 keysReplicated=2397 queued=65/10000000 replicators=2 source="172.20.33.50:13511" target="10.218.75.68:13502"
INFO[1375] status report                                 eventsArrived=1950 eventsProcessed=2015 keysQueried=2060 keysReplicated=2114 queued=0/10000000 replicators=2 source="172.20.33.50:13511" target="10.218.75.68:13502"
INFO[1380] status report                                 eventsArrived=487 eventsProcessed=487 keysQueried=482 keysReplicated=481 queued=0/10000000 replicators=2 source="172.20.33.50:13511" target="10.218.75.68:13502"
INFO[1385] status report                                 eventsArrived=352 eventsProcessed=352 keysQueried=349 keysReplicated=321 queued=0/10000000 replicators=2 source="172.20.33.50:13511" target="10.218.75.68:13502"
...
```
