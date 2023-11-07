package main

import (
	"fmt"

	"github.com/Ankr-Shanghai/chainkv/client"
)

func main() {
	fmt.Println("test iterator")

	option := &client.Option{
		Host: "127.0.0.1",
		Port: "4321",
		Size: 1,
	}

	cli, err := client.NewClient(option)
	if err != nil {
		fmt.Println("NewClient error:", err)
		return
	}
	defer cli.Close()

	iter, _ := cli.NewIter(nil, nil)
	defer iter.Close()

	fmt.Printf("iter is %v\n", iter.Next())

	// for iter.Next() {

	// }

}
