package main

import (
	"fmt"
	"godis"
)

func main() {
	server, err := godis.NewServer(":9000")
	if err != nil {
		panic(err)
	}
	server.Options.Eviction.Policy = godis.VolatileRandom
	server.Options.Eviction.MaxOffHeapSize = 50
	if err := server.Serve(); err != nil {
		panic(err)
	}
	fmt.Println("exit")
}
