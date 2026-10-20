package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"unicode/utf8"

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

	var input string
	Scanner := bufio.NewScanner(os.Stdin)

	go RecieveMessages(stream)

	for Scanner.Scan() {
		input = Scanner.Text()
		if input == "exit" {
			break
		}
		PostMessage(stream, *id, input)
	}
	stream.Send(&proto.ClientEvent{
		ClientId:    *id,
		LamportTime: 1,
		Event:       &proto.ClientEvent_Leave{Leave: &proto.LeaveRequest{}},
	})
	stream.CloseSend()

}
func PostMessage(stream proto.ChatService_ChatClient, id string, content string) {
	if !utf8.ValidString(content) {
		fmt.Println("Message is not valid UTF-8 and was not sent.")
		return
	}
	if utf8.RuneCountInString(content) > 128 {
		fmt.Println("Message exceeds 128 characters and was not sent.")
		return
	}
	err := stream.Send(&proto.ClientEvent{
		ClientId:    id,
		LamportTime: 1,
		Event:       &proto.ClientEvent_Publish{Publish: &proto.PublishRequest{Content: content}},
	})
	if err != nil {
		log.Fatalf("Could not send message: %v", err)
	}
}

func RecieveMessages(stream proto.ChatService_ChatClient) {
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatalf("Connection ended: %v", err)
		}
		switch event.GetType() {
		case proto.EventType_PUBLISHED:
			fmt.Printf("[t=%d] %s: %s\n", event.LamportTime, event.ClientId, event.Content)
		default: // JOINED / LEFT: content holds the full "Participant X ..." text
			fmt.Printf("[t=%d] %s\n", event.GetLamportTime(), event.GetContent())
		}
	}
}
