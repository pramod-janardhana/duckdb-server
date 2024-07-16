package config

import (
	"log"
	"os"
	"strconv"
)

func getEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("Missing required environment variable: %s", key)
	}
	return val
}

func getEnvAsInt(key string) int {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("Missing required environment variable: %s", key)
	}

	v, err := strconv.Atoi(val)
	if err != nil {
		log.Fatalf("Error parsing environment variable: %s, err: %v", key, err)
	}
	return v
}

type serviceConfig struct {
	Host      string
	Port      int
	ProfDir   string
	DuckDBDir string
}

var GRPC = serviceConfig{
	ProfDir:   "/Users/pramodj/Documents/Projects/github/pramod-janardhana/go-duckdb/prof",
	DuckDBDir: "/Users/pramodj/Documents/Projects/github/pramod-janardhana/go-duckdb/duckdb",
}

var (
	HOST string
	PORT int
)

var (
	TEMP_DOWNLOAD_DIR string
	TEMP_PROF_DIR     string
	TEMP_DUCKDB_DIR   string
	DUCKDB_DIR        string
)

var CHUNK_SIZE int
var FILE_CHUNK_SIZE int

func GetConfig() {
	HOST = getEnv("HOST")
	PORT = getEnvAsInt("PORT")

	TEMP_PROF_DIR = getEnv("TEMP_PROF_DIR")
	TEMP_DUCKDB_DIR = getEnv("TEMP_DUCKDB_DIR")
	DUCKDB_DIR = getEnv("DUCKDB_DIR")
	TEMP_DOWNLOAD_DIR = getEnv("TEMP_DOWNLOAD_DIR")

	CHUNK_SIZE = getEnvAsInt("CHUNK_SIZE")
	FILE_CHUNK_SIZE = getEnvAsInt("FILE_CHUNK_SIZE")
}
