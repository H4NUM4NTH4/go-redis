package server

import (
	"fmt"
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

	// Create store with file path for persistence
	s := store.NewStore("dump.json")

	// Handle graceful shutdown
	// When you press Ctrl+C, save data before exiting
	go handleShutdown(s)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		fmt.Println("New client connected:", conn.RemoteAddr())
		go handleClient(conn, s)
	}
}

// handleShutdown listens for Ctrl+C and saves data before exiting
// Like a cashier who closes the register properly before leaving
func handleShutdown(s *store.Store) {
	// Create a channel that receives OS signals
	quit := make(chan os.Signal, 1)

	// Tell Go: "when user presses Ctrl+C, send it to quit channel"
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Block here until signal arrives
	<-quit

	fmt.Println("\n🛑 Shutting down server...")

	// Save data before exiting
	if err := s.Save(); err != nil {
		fmt.Println("Failed to save data:", err)
	}

	fmt.Println("✅ Data saved. Goodbye!")
	os.Exit(0)
}