package main

import (
	"log"
	"net"
	"sync"

	proto "chit-chat/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	proto.UnimplementedChatServiceServer
	
	mu      sync.Mutex
	clients map[string]chan *proto.ServerEvent
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
		s.mu.Unlock()
	}()

	log.Printf("[Server] event=CONNECT client=%s", id)

	return nil
}

func main() {
	lis, err := net.Listen("tcp", "localhost:5051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	server := grpc.NewServer()
	proto.RegisterChatServiceServer(server, &Server{
	clients: make(map[string]chan *proto.ServerEvent),
	})


	log.Println("Server is running on localhost:5051")
	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

}
