package main

import (
	"os"

	"go-nosql-db/pkg/engine"
)

func main() {
	_, err := engine.NewDal("test.db", uint(os.Getpagesize()))
	panicOnError(err)
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
