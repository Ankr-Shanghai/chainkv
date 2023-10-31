package main

import (
	"fmt"

	"github.com/Ankr-Shanghai/chainkv/client"
)

func main() {
	fmt.Println("test iterator")

	var (
		key1 = []byte("key1")
		key2 = []byte("key2")
		key3 = []byte("key3")
		val1 = []byte("val1")
		val2 = []byte("val2")
		val3 = []byte("val3")
	)

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

	cli.Put(key1, val1)
	cli.Put(key2, val2)
	cli.Put(key3, val3)

	// create iterator
	iter, err := cli.NewIter([]byte("key"), nil)
	if err != nil {
		fmt.Println("NewIter error:", err)
		return
	}
	defer iter.Close()

	for iter.Next() {
		fmt.Printf("k: %s, v: %s\n", iter.Key(), iter.Value())
	}

}
