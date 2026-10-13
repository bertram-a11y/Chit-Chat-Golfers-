package main

import (
	"context"
	"log"
	"net"

	proto "chit-chat/grpc"

	"google.golang.org/grpc"
)

type Server struct {
	proto.UnimplementedGreeterServer
}

func (s *Server) Greeter(ctx context.Context, req *proto.GreeterRequest) (*proto.GreeterResponse, error) {
	return &proto.GreeterResponse{
		Message: "Hello, " + req.GetName() + "!",
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", "localhost:5051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	server := grpc.NewServer()
	proto.RegisterGreeterServer(server, &Server{})

	log.Println("Server is running on localhost:5051")
	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

}
