package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"unicode/utf8"

	proto "chit-chat/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	proto.UnimplementedChatServiceServer

	mu      sync.Mutex
	clients map[string]chan *proto.ServerEvent
	clock   int64 //lamport clock
}

// receive updates the Lamport clock when an event arrives from a client.
func (s *Server) receive(t int64) {
	s.mu.Lock()
	s.clock = max(s.clock, t) + 1
	s.mu.Unlock()
}

func (s *Server) broadcast(event *proto.ServerEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clock++
	event.LamportTime = s.clock

	switch event.Type {
	case proto.EventType_JOINED:
		event.Content = fmt.Sprintf("Participant %s joined Chit Chat at logical time %d", event.ClientId, s.clock)
	case proto.EventType_LEFT:
		event.Content = fmt.Sprintf("Participant %s left Chit Chat at logical time %d", event.ClientId, s.clock)
	}

	for id, ch := range s.clients {
		select {
		case ch <- event:
			log.Printf("[Server] event=DELIVER client=%s lamport=%d", id, event.LamportTime)
		default: // buffer full, don't block the others
			log.Printf("[Server] event=DROP client=%s lamport=%d", id, event.LamportTime)
		}
	}
}

func (s *Server) Chat(stream proto.ChatService_ChatServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}

	id := first.GetClientId()

	if first.GetJoin() == nil || id == "" {
		return status.Error(codes.InvalidArgument, "the first event must be a join with a client_id")
	}

	s.receive(first.GetLamportTime())

	out := make(chan *proto.ServerEvent, 64)

	s.mu.Lock()
	if _, taken := s.clients[id]; taken {
		s.mu.Unlock()
		return status.Errorf(codes.AlreadyExists, "client id %q is already in use", id)
	}
	s.clients[id] = out
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.clients, id)
		close(out) //stops the go routine
		s.mu.Unlock()
		log.Printf("[Server] event=DISCONNECT client=%s", id)
		log.Printf("[Server] event=BROADCAST_LEAVE client=%s", id)
		s.broadcast(&proto.ServerEvent{Type: proto.EventType_LEFT, ClientId: id}) // S6
	}()

	log.Printf("[Server] event=CONNECT client=%s", id)
	log.Printf("[Server] event=BROADCAST_JOIN client=%s", id)
	s.broadcast(&proto.ServerEvent{Type: proto.EventType_JOINED, ClientId: id}) // S5

	// Start a goroutine to send events to the client
	go func() {
		for event := range out {
			if err := stream.Send(event); err != nil {
				return
			}
		}
	}()

	// Receive events from the client
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		s.receive(event.GetLamportTime())
		if event.GetLeave() != nil {
			return nil // the deferred func broadcasts the leave
		}

		if event.GetLeave() != nil {
			return nil // the deferred func broadcasts the leave
		}

		if p := event.GetPublish(); p != nil {
			msg := p.GetContent()
			if !utf8.ValidString(msg) || utf8.RuneCountInString(msg) > 128 {
				log.Printf("[Server] event=INVALID_MESSAGE client=%s", id)
				continue
			}
			log.Printf("[Server] event=PUBLISH client=%s content=%q", id, msg)
			s.broadcast(&proto.ServerEvent{
				Type:     proto.EventType_PUBLISHED,
				ClientId: id,
				Content:  msg,
			})
		}
	}
}

func main() {
	if err := os.MkdirAll("logs", 0o755); err != nil {
		log.Fatalf("could not create logs directory: %v", err)
	}

	f, err := os.OpenFile("logs/server.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Fatalf("could not open log file: %v", err)
	}

	defer f.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, f))

	lis, err := net.Listen("tcp", "localhost:5051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	server := grpc.NewServer()
	proto.RegisterChatServiceServer(server, &Server{
		clients: make(map[string]chan *proto.ServerEvent),
	})

	log.Printf("[Server] event=START address=localhost:5051")

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Printf("[Server] event=SHUTDOWN server stopping")
		server.Stop()
	}()

	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

}
