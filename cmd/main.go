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
	if err := server.Serve(); err != nil {
		panic(err)
	}
	fmt.Println("exit")
}
