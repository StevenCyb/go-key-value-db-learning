package main

import (
	"go-nosql-db/pkg/engine"
)

func main() {
	_, err := engine.NewDal("test.db")
	panicOnError(err)
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
