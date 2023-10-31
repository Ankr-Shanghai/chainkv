package main

import (
	"fmt"
	"strings"

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

	var puts = fmt.Sprintf(strings.Repeat("a", 32))

	// fmt.Printf("puts len: %d \n", len(puts))

	err = client.Put([]byte("key"), []byte(puts))
	if err != nil {
		fmt.Println("write failed: ", err)
		return
	}

	// err = client.Delete([]byte("key"))
	// if err != nil {
	// 	fmt.Println("delete failed: ", err)
	// 	return
	// }

	gets, err := client.Get([]byte("key"))
	if err != nil {
		fmt.Println("write get failed: ", err)
		return
	}
	fmt.Printf("return len: %d \n", len(gets))

}
