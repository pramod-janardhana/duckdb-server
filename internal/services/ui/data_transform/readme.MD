# This package contains the .proto file and functions for consuming the duckDb server capabilities by a backend server.

# Run the below command to generate client and server code
protoc --go_out=. --go_opt=paths=source_relative \
 --go-grpc_out=. --go-grpc_opt=paths=source_relative \
 data_transform_ui.proto
