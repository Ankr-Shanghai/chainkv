package main

import (
	"fmt"

	"github.com/Ankr-Shanghai/chainkv/client"
)

func main() {

	opt := &client.Option{
		Host: "127.0.0.1",
		Port: "4321",
		Size: 1,
	}

	client, err := client.NewClient(opt)
	if err != nil {
		panic(err)
	}

	defer client.Close()
	for i := 0; i < 10; i++ {
		seq, _ := client.Sequence()
		fmt.Printf("seq: %d \n", seq)
	}
}
