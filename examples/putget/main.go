package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"

	"github.com/Ankr-Shanghai/chainkv/client"
)

func main() {

	opt := &client.Option{
		Host: "127.0.0.1",
		// Host: "23.92.177.82",
		Port: "4321",
		Size: 1,
	}

	client, err := client.NewClient(opt)
	if err != nil {
		panic(err)
	}

	defer client.Close()

	// var puts = fmt.Sprintf(strings.Repeat("a", 16*1024))
	// err = client.Put([]byte("key"), []byte(puts))
	// if err != nil {
	// 	fmt.Println("write failed: ", err)
	// 	return
	// }

	// rs, err := client.Get([]byte("key"))
	// if err != nil {
	// 	fmt.Println("delete failed: ", err)
	// 	return
	// }
	// fmt.Printf("return len: %d \n", len(rs))

	src := "657468657265756d2d67656e657369732d0d21840abff46b96c84b2ac9e10e4f5cdaeb5693cb665db62a2f3b02d2d57b5b"

	dst := make([]byte, hex.DecodedLen(len(src)))
	hex.Decode(dst, []byte(src))
	gets, err := client.Get(dst)
	if err != nil {
		fmt.Println("write get failed: ", err)
		return
	}
	fmt.Printf("return len: %d  val=%x\n", len(gets), md5.Sum(gets))

	// src = "6800000000000000006e"

	// dst = make([]byte, hex.DecodedLen(len(src)))
	// hex.Decode(dst, []byte(src))
	// fmt.Printf("dst: %s\n", dst)
	// gets, err = client.Get(dst)
	// if err != nil {
	// 	fmt.Println("write get failed: ", err)
	// 	return
	// }
	// fmt.Printf("return len: %d hash=%x\n", len(gets), md5.Sum(gets))
}
