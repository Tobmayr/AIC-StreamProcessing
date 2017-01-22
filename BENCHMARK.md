# Benchmarks

Methodoloy
----------

Our benchmark process was aided by a custom listener in the topology which
counts the number of tuples entering and leaving the topology and provides a
simple static in form of tuples processed per second.

We gathered benchmark data from our large, merged data set:

```
$ head -50000 taxi_data.csv > benchmark_data.csv
```

The topologies were started using their respective gradle commands dedicated
for benchmarking:
```
$ gradle bmTopology # benchmark baseline topology
$ gradle bmOptimizedTopology # benchmark optimized topology
```

After starting the topology we submitted the benchmark data using a submission
speed of 1000000:
```
$ gradle runDataprovider -Pspeed=1000000 -Pdata=./testdata/benchmark_data.csv
```

Furthermore, in order to isolate the performance from the dashboard we disabled
communication between topology and the dashboard for the duration of the
benchmark.


Results
-------


| Topology  | #partitions | parallelism hint | tuples/second |
|-----------|-------------|------------------|---------------|
| Baseline  | 1           | no               | 1972.64       |
| Optimized | 1           | no               | 3028.46       |
| Optimized | 3           | no               | 3553.12       |
| Optimized | 5           | no               | 3572.20       |
| Optimized | 3           | 3                | 8542.46       |


