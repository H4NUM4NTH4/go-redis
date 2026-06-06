package server

import (
	"fmt"
	"Redis-go/resp"
	"Redis-go/store"
	"net"
	"strconv"
	"strings"
	"time"
)

func handleClient(conn net.Conn, s *store.Store) {
	defer conn.Close()

	reader := resp.NewReader(conn)

	for {
		args, err := reader.ReadCommand()
		if err != nil {
			fmt.Println("Client disconnected:", conn.RemoteAddr())
			return
		}

		if len(args) == 0 {
			continue
		}

		command := strings.ToUpper(args[0])
		fmt.Printf("Command: %s, Args: %v\n", command, args[1:])

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
		case "EXPIRE":
			handleExpire(conn, s, args)
		case "TTL":
			handleTTL(conn, s, args)
		case "PERSIST":
			handlePersist(conn, s, args)
		case "SETEX":
			handleSetEx(conn, s, args)
		default:
			resp.WriteError(conn, fmt.Sprintf("unknown command '%s'", command))
		}
	}
}

func handlePing(conn net.Conn) {
	resp.WriteSimpleString(conn, "PONG")
}

func handleSet(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 3 {
		resp.WriteError(conn, "SET requires 2 arguments: key and value")
		return
	}
	s.Set(args[1], args[2])
	resp.WriteSimpleString(conn, "OK")
}

func handleGet(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 2 {
		resp.WriteError(conn, "GET requires 1 argument: key")
		return
	}
	value, ok := s.Get(args[1])
	if !ok {
		resp.WriteNull(conn)
		return
	}
	resp.WriteBulkString(conn, value)
}

func handleDel(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 2 {
		resp.WriteError(conn, "DEL requires 1 argument: key")
		return
	}
	if s.Del(args[1]) {
		conn.Write([]byte(":1\r\n"))
	} else {
		conn.Write([]byte(":0\r\n"))
	}
}

func handleExists(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 2 {
		resp.WriteError(conn, "EXISTS requires 1 argument: key")
		return
	}
	if s.Exists(args[1]) {
		conn.Write([]byte(":1\r\n"))
	} else {
		conn.Write([]byte(":0\r\n"))
	}
}

// handleExpire sets expiry in seconds on a key
func handleExpire(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 3 {
		resp.WriteError(conn, "EXPIRE requires 2 arguments: key and seconds")
		return
	}

	// Convert seconds string to integer
	// Like Integer.parseInt() in Java
	seconds, err := strconv.Atoi(args[2])
	if err != nil {
		resp.WriteError(conn, "seconds must be a number")
		return
	}

	// Convert seconds to a Go duration
	// time.Second is 1 second, multiply by how many seconds we want
	duration := time.Duration(seconds) * time.Second

	if s.Expire(args[1], duration) {
		conn.Write([]byte(":1\r\n"))
	} else {
		conn.Write([]byte(":0\r\n"))
	}
}

// handleTTL returns remaining seconds for a key
func handleTTL(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 2 {
		resp.WriteError(conn, "TTL requires 1 argument: key")
		return
	}

	ttl := s.TTL(args[1])
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", ttl)))
}

// handlePersist removes expiry from a key
func handlePersist(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 2 {
		resp.WriteError(conn, "PERSIST requires 1 argument: key")
		return
	}

	if s.Persist(args[1]) {
		conn.Write([]byte(":1\r\n"))
	} else {
		conn.Write([]byte(":0\r\n"))
	}
}

// handleSetEx sets a key with value AND expiry in one command
func handleSetEx(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 4 {
		resp.WriteError(conn, "SETEX requires 3 arguments: key, seconds, value")
		return
	}

	seconds, err := strconv.Atoi(args[2])
	if err != nil {
		resp.WriteError(conn, "seconds must be a number")
		return
	}

	duration := time.Duration(seconds) * time.Second
	s.SetEx(args[1], args[3], duration)
	resp.WriteSimpleString(conn, "OK")
}