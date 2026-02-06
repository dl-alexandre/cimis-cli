module github.com/dl-alexandre/cimis-cli

go 1.21

require github.com/dl-alexandre/cimis-tsdb v0.0.0

require (
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/mattn/go-sqlite3 v1.14.22 // indirect
)

replace github.com/dl-alexandre/cimis-tsdb => ../../../cimis-tsdb
