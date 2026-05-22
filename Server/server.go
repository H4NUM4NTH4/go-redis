package server

import (
	"fmt"
	"Redis-go/store"
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

	// Create ONE store — shared across ALL clients
	// Like one whiteboard for the whole restaurant
	s := store.NewStore()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		fmt.Println("New client connected:", conn.RemoteAddr())

		// Pass the store to each client handler
		go handleClient(conn, s)
	}
}