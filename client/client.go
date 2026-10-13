package main

import (
	"context"
	"log"
	"os"
	"time"

	proto "chit-chat/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:5051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Not Working: %v", err)
	}

	client := proto.NewGreeterClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	username, err := os.Hostname()
	if err != nil {
		log.Fatalf("Failed to get hostname: %v", err)
	}

	res, err := client.Greeter(ctx, &proto.GreeterRequest{Name: username})
	if err != nil {
		log.Fatalf("Error calling Greeter: %v", err)
	}

	log.Printf("%v", res)
}
