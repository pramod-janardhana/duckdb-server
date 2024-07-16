protoc --go_out=. --go_opt=paths=source_relative \
 --go-grpc_out=. --go-grpc_opt=paths=source_relative \
 internal/services/grpc_arrow/data_transform/data_tranform_arrow.proto

podman build -t duckdb-server -f Dockerfile . 

docker run -v /Users/pramodj/Desktop/podman/duckdb:/tmp/duckdb -v /Users/pramodj/Desktop/podman/prof:/tmp/prof -v /Users/pramodj/Desktop/podman/tmpdir:/tmp/tmpdir -v /Users/pramodj/Desktop/dataset:/tmp/dataset -p 9006:9006 --env-file /Users/pramodj/Documents/Projects/github/pramod-janardhana/duckdb-server/env_podman -it duckdb-server:latest
