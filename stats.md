Tested on M2 MacBook Pro
### JSON stream via gRPC
## Size | Chunk size | Execution time
## 1M   | 50k        | 14-15s
## 5M   | 50k        | 1m 70s
## 10M  | 50k        | 5m 25s
## 20M  | 50k        | ~11m

### Arrow stream via gRPC
## Size | Chunk size | Execution time (containerized) | Execution time(with grouping set)
## 1M   | 50k        | 5s                             | 2.7m -> 40M
## 5M   | 50k        | 18s                            | ~14m -> 184M
## 10M  | 50k        | 34s                            | OS killed the process 10m -> 364M
## 20M  | 50k        | 64s

### Parquet stream with encryption and gzip compression via gRPC
## Size | Chunk size | Execution time (containerized)
## 1M   | 30MB       | 7s
## 5M   | 30MB       | 24-25s
## 10M  | 30MB       | 50s
## 20M  | 30MB       | 98s

### REST
## Size | Execution time
## 1M   | failed to run
