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

	batch, err := client.NewBatch()
	if err != nil {
		panic(err)
	}
	defer batch.Close()

	batch.Put([]byte("key1"), []byte("value1"))
	batch.Put([]byte("key2"), []byte("value2"))
	batch.Put([]byte("key3"), []byte("value3"))
	batch.Put([]byte("key4"), []byte("value4"))

	err = batch.Write()
	if err != nil {
		panic(err)
	}

	val, err := client.Get([]byte("key1"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("key1: %s\n", val)

}
