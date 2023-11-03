package main

import (
	"fmt"

	"github.com/Ankr-Shanghai/chainkv/client"
	"github.com/ethereum/go-ethereum/common"
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

	rootHashString := "0x919fcc7ad870b53db0aa76eb588da06bacb6d230195100699fc928511003b422"

	rootHash := common.HexToHash(rootHashString)

	gets, err := client.Has(rootHash.Bytes())
	if err != nil {
		fmt.Println("has failed: ", err)
		return
	}
	fmt.Printf("return  val=%v\n", gets)
	// fmt.Printf("return len: %d  val=%x\n", len(gets), md5.Sum(gets))
}
