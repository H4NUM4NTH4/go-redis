package server

import (
	"fmt"
	"net"
)

// StartServer starts our TCP server
func StartServer() {
	// Step 1: Open the door at port 6379
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Redis server listening on port 6379...")

	// Step 2: Keep the door open forever, waiting for customers
	for {
		// Wait until a client connects
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		fmt.Println("New client connected:", conn.RemoteAddr())

		// Step 3: Handle each client in a separate goroutine
		go handleClient(conn)
	}
}

// handleClient handles one connected client
func handleClient(conn net.Conn) {
	defer conn.Close() // When this function ends, close the connection

	// A buffer — like a bucket to collect incoming data
	buf := make([]byte, 1024)

	for {
		// Read what the client sends into our bucket
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Client disconnected:", conn.RemoteAddr())
			return
		}

		// Print what we received
		message := string(buf[:n])
		fmt.Printf("Received from client: %s\n", message)

		// For now, just echo it back
		conn.Write([]byte("Got it!\n"))
	}
}
