Tested on M2 MacBook Pro

### Web socket

## Size | Chunk size | Execution time

## 1M | 25k | 14s

## 5M | 25k | 1m 20s

## 10M | 25k | ~4m

## 20M | 25k | ~19m

### gRPC

## Size | Chunk size | Execution time

## 1M | 50k | 14-15s

## 5M | 50k | 1m 70s

## 10M | 50k | 5m 25s

## 20M | 50k | ~11m

### arrow via gRPC

## Size | Chunk size | Execution time |

## 1M | 50k | 7-8s | 2.7m -> 40M

## 5M | 50k | 47-50s | ~14m -> 184M

## 10M | 50k | 2.5m | OS killed the process 10m -> 364M

## 20M | 50k | ~9m

### REST

## Size | Execution time

1M | failed to run
