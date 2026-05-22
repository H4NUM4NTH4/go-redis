package server

import (
	"fmt"
	"Redis-go/resp"
	"Redis-go/store"
	"net"
	"strings"
)

// handleClient reads commands from a client and responds
func handleClient(conn net.Conn, s *store.Store) {
	defer conn.Close()

	reader := resp.NewReader(conn)

	for {
		// Read one full command
		args, err := reader.ReadCommand()
		if err != nil {
			fmt.Println("Client disconnected:", conn.RemoteAddr())
			return
		}

		if len(args) == 0 {
			continue
		}

		// Convert command to uppercase so "set" and "SET" both work
		// Like: "set".toUpperCase() in Java
		command := strings.ToUpper(args[0])

		fmt.Printf("Command: %s, Args: %v\n", command, args[1:])

		// Route the command to the right handler
		// Like a switch statement router
		switch command {
		case "PING":
			handlePing(conn)
		case "SET":
			handleSet(conn, s, args)
		case "GET":
			handleGet(conn, s, args)
		case "DEL":
			handleDel(conn, s, args)
		case "EXISTS":
			handleExists(conn, s, args)
		default:
			resp.WriteError(conn, fmt.Sprintf("unknown command '%s'", command))
		}
	}
}

// handlePing responds with PONG
func handlePing(conn net.Conn) {
	resp.WriteSimpleString(conn, "PONG")
}

// handleSet handles SET key value
func handleSet(conn net.Conn, s *store.Store, args []string) {
	// SET needs exactly 2 arguments: key and value
	if len(args) < 3 {
		resp.WriteError(conn, "SET requires 2 arguments: key and value")
		return
	}

	key := args[1]
	value := args[2]

	s.Set(key, value)
	resp.WriteSimpleString(conn, "OK")
}

// handleGet handles GET key
func handleGet(conn net.Conn, s *store.Store, args []string) {
	// GET needs exactly 1 argument: key
	if len(args) < 2 {
		resp.WriteError(conn, "GET requires 1 argument: key")
		return
	}

	key := args[1]

	value, ok := s.Get(key)
	if !ok {
		// Key doesn't exist — return null (Redis returns nil for missing keys)
		resp.WriteNull(conn)
		return
	}

	resp.WriteBulkString(conn, value)
}

// handleDel handles DEL key
func handleDel(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 2 {
		resp.WriteError(conn, "DEL requires 1 argument: key")
		return
	}

	key := args[1]
	existed := s.Del(key)

	if existed {
		// Redis returns :1 if key was deleted
		conn.Write([]byte(":1\r\n"))
	} else {
		// Redis returns :0 if key didn't exist
		conn.Write([]byte(":0\r\n"))
	}
}

// handleExists handles EXISTS key
func handleExists(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 2 {
		resp.WriteError(conn, "EXISTS requires 1 argument: key")
		return
	}

	key := args[1]

	if s.Exists(key) {
		conn.Write([]byte(":1\r\n")) // exists
	} else {
		conn.Write([]byte(":0\r\n")) // doesn't exist
	}
}