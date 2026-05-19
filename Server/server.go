package server

import (
	"fmt"
	"Redis-go/resp"
	"net"
)

func StartServer() {
	listener, err := net.Listen("tcp", ":7379")
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Redis server listening on port 7379...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		fmt.Println("New client connected:", conn.RemoteAddr())
		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	defer conn.Close()

	// Wrap the connection in our RESP reader
	// Like hiring a translator who understands the Redis language
	reader := resp.NewReader(conn)

	for {
		// Read one full command from the client
		args, err := reader.ReadCommand()
		if err != nil {
			fmt.Println("Client disconnected:", conn.RemoteAddr())
			return
		}

		// Print what command we received
		fmt.Printf("Command received: %v\n", args)

		// For now, just reply OK to everything
		resp.WriteSimpleString(conn, "OK")
	}
}