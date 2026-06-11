package server

import (
	"fmt"
	"Redis-go/pubsub"
	"Redis-go/store"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func StartServer() {
	listener, err := net.Listen("tcp", ":7379")
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Redis server listening on port 7379...")

	// Create store and pubsub manager
	// Both are shared across ALL clients
	s := store.NewStore("dump.json")
	ps := pubsub.NewPubSub() // ← NEW

	go handleShutdown(s)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		fmt.Println("New client connected:", conn.RemoteAddr())

		// Pass both store AND pubsub to each client
		go handleClient(conn, s, ps)
	}
}

func handleShutdown(s *store.Store) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("\n🛑 Shutting down server...")
	if err := s.Save(); err != nil {
		fmt.Println("Failed to save data:", err)
	}
	fmt.Println("✅ Data saved. Goodbye!")
	os.Exit(0)
}