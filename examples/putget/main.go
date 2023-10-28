package main

import (
	"log"
	"net"

	"github.com/Allenxuxu/gev/plugins/protobuf"
	"github.com/Ankr-Shanghai/chainkv/client/pb"
	"google.golang.org/protobuf/proto"
)

func main() {
	conn, e := net.Dial("tcp", "localhost:4321")
	if e != nil {
		log.Fatal(e)
	}
	defer conn.Close()

	// put

	// req := &pb.PutRequest{
	// 	Key:   []byte("key"),
	// 	Value: []byte("vvvvvv"),
	// }

	// reqs, err := proto.Marshal(req)
	// if err != nil {
	// 	log.Fatal("marshal", err)
	// }

	// buffer := protobuf.PackMessage("PutRequest", reqs)

	// _, err = conn.Write(buffer)
	// if err != nil {
	// 	log.Fatal("write put", err)
	// }

	rspBuf := make([]byte, 1024)
	// n, err := conn.Read(rspBuf)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// rsp := &pb.PutResponse{}
	// err = proto.Unmarshal(rspBuf[:n], rsp)
	// if err != nil {
	// 	log.Fatal("putrsp um == ", err)
	// }
	// log.Printf("rsp: %+v", rsp)

	// get

	reqg := &pb.GetRequest{
		Key: []byte("key"),
	}
	reqgs, err := proto.Marshal(reqg)
	if err != nil {
		log.Fatal(err)
	}

	buffer := protobuf.PackMessage("GetRequest", reqgs)
	_, err = conn.Write(buffer)
	if err != nil {
		log.Fatal(err)
	}

	rspg := &pb.GetResponse{}
	n, err := conn.Read(rspBuf)
	if err != nil {
		log.Fatal(err)
	}
	err = proto.Unmarshal(rspBuf[:n], rspg)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("rsp: %+v", rspg)
}
