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
	"sync"

	proto "chit-chat/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	id := flag.String("id", "", "your participant name")
	flag.Parse()

	f, err := os.OpenFile(*id+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Could not open log file: %v", err)
	}
	defer f.Close()
	log.SetOutput(f)

	clock := &LamportClock{}
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
		LamportTime: clock.Tick(),
		Event:       &proto.ClientEvent_Join{Join: &proto.JoinRequest{}},
	})
	if err != nil {
		log.Fatalf("Could not send join: %v", err)
	}

	go RecieveMessages(stream, clock, *id)
	
	var input string
	Scanner := bufio.NewScanner(os.Stdin)

	for Scanner.Scan() {
		input = Scanner.Text()
		if input == "exit" {
			break
		}
		PostMessage(stream, clock, *id, input)
	}
	stream.Send(&proto.ClientEvent{
		ClientId:    *id,
		LamportTime: clock.Tick(),
		Event:       &proto.ClientEvent_Leave{Leave: &proto.LeaveRequest{}},
	})
	stream.CloseSend()

}
func PostMessage(stream proto.ChatService_ChatClient, clock *LamportClock, id string, content string) {
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
		LamportTime: clock.Tick(),
		Event:       &proto.ClientEvent_Publish{Publish: &proto.PublishRequest{Content: content}},
	})
	if err != nil {
		log.Fatalf("Could not send message: %v", err)
	}
}

func RecieveMessages(stream proto.ChatService_ChatClient, clock *LamportClock, id string) {
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatalf("Connection ended: %v", err)
		}
		local := clock.Update(event.GetLamportTime())
		
		var text string

		switch event.GetType() {
			case proto.EventType_PUBLISHED:
				text = fmt.Sprintf("%s: %s", event.GetClientId(), event.GetContent())
			default: // JOINED / LEFT: content holds the full "Participant X ..." text
				text = event.GetContent()
		}

		fmt.Printf("[t=%d] %s\n", event.GetLamportTime(), text)
		log.Printf("[Client] event=RECEIVE client=%s lamport=%d local=%d content=%q",id, event.GetLamportTime(), local, text)
	}
}

	type LamportClock struct {
		mu sync.Mutex
		time int64
	}

	func (c *LamportClock) Update(t int64) int64 {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.time = max(c.time, t) + 1
		return c.time
	}

	func (c *LamportClock) Tick() int64 {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.time++
		return c.time
	}
