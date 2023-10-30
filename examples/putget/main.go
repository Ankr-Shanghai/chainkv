package main

import (
	"fmt"

	"github.com/Ankr-Shanghai/chainkv/client"
)

func main() {
	client := client.NewClient("127.0.0.1:4321")

	// var puts = fmt.Sprintf(strings.Repeat("a", 1024*1024))

	// fmt.Printf("puts len: %d \n", len(puts))

	// err := client.Put([]byte("key"), []byte(puts))
	// if err != nil {
	// 	fmt.Println("write failed: ", err)
	// 	return
	// }

	err := client.Delete([]byte("key"))
	if err != nil {
		fmt.Println("delete failed: ", err)
		return
	}

	gets, err := client.Get([]byte("key"))
	if err != nil {
		fmt.Println("write get failed: ", err)
		return
	}
	fmt.Printf("return len: %d \n", len(gets))

}
