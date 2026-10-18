package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"

	proto "chit-chat/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	id := flag.String("id", "", "your participant name")
	flag.Parse()

	conn, err := grpc.NewClient("localhost:5051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}
	defer conn.Close()

	stream, err := proto.NewChatServiceClient(conn).Chat(context.Background())
	if err != nil {
		log.Fatalf("Could not open chat stream: %v", err)
	}

	err = stream.Send(&proto.ClientEvent{
		ClientId:    *id,
		LamportTime: 1,
		Event:       &proto.ClientEvent_Join{Join: &proto.JoinRequest{}},
	})
	if err != nil {
		log.Fatalf("Could not send join: %v", err)
	}

	for {
		ev, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatalf("Connection ended: %v", err)
		}
		fmt.Printf("[t=%d] %s\n", ev.GetLamportTime(), ev.GetContent())
	}
}
