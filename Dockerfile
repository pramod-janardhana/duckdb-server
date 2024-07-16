FROM golang:1.22

RUN cd /tmp
RUN mkdir duckdb_tmp
RUN mkdir duckdb

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy the source from the current directory to the Working Directory inside the container
COPY go.mod ./
COPY go.sum ./
COPY . ./

# Download all dependencies.
RUN go mod tidy

# Build the Go app
RUN go build -o main ./cmd

# Expose port 50051 to the outside world
EXPOSE 9006

# Command to run the executable
CMD ["./main"]
