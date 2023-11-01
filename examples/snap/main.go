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
		fmt.Println(err)
		return
	}
	defer client.Close()

	client.Put([]byte("key"), []byte("val1111"))
	client.Put([]byte("key1"), []byte("value1"))
	client.Put([]byte("key2"), []byte("value2"))
	client.Put([]byte("key3"), []byte("value3"))

	snap, err := client.NewSnap()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer snap.Release()

	rsp, err := snap.Get([]byte("key"))
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("rsp: %s\n", rsp)

}
