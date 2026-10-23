# Project README

A simple Go application with a server and client.

## Run the program

1. Start the server:

   ```bash
   go run server/server.go
   ```

2. Start a client with your name:

   ```bash
   go run client/client.go -id <your name>
   ```

   Example:

   ```bash
   go run client/client.go -id alice
   ```

## Notes

- Run the server before starting any clients.
- The `-id` flag sets the client identifier.
