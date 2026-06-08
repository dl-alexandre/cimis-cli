package main

import "log"

var (
	logFatal  = log.Fatal
	logFatalf = log.Fatalf
)

func fatalIfErr(err error) {
	if err != nil {
		logFatal(err)
	}
}
