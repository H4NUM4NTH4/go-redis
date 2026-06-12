package server

import (
	"Redis-go/pubsub"
	"Redis-go/resp"
	"Redis-go/store"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// Transaction holds the state for one client's transaction
// Each client has their own — completely independent
type Transaction struct {
	active   bool       // true = inside MULTI block
	commands [][]string // queued commands waiting for EXEC
}

func handleClient(conn net.Conn, s *store.Store, ps *pubsub.PubSub) {
	defer conn.Close()
	reader := resp.NewReader(conn)
	clientID := conn.RemoteAddr().String()
	defer ps.UnsubscribeAll(clientID)

	// Each client starts with no active transaction
	// Like a shopper who hasn't picked up a cart yet
	tx := &Transaction{
		active:   false,
		commands: [][]string{},
	}

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

		// ─────────────────────────────────────────
		// TRANSACTION INTERCEPT LOGIC
		// ─────────────────────────────────────────
		//
		// If we're inside a MULTI block, most commands
		// should be QUEUED, not executed immediately.
		//
		// Exceptions — these always execute immediately:
		// EXEC    → execute the queue
		// DISCARD → abandon the queue
		// MULTI   → error (can't nest transactions)
		if tx.active {
			switch command {
			case "EXEC":
				handleExec(conn, s, ps, tx, clientID)
			case "DISCARD":
				handleDiscard(conn, tx)
			case "MULTI":
				// Nested MULTI is not allowed
				resp.WriteError(conn, "ERR MULTI calls can not be nested")
			default:
				// Queue the command instead of executing it
				tx.commands = append(tx.commands, args)
				// Tell client it's been queued
				conn.Write([]byte("+QUEUED\r\n"))
			}
			continue
		}

		// Normal execution (not in a transaction)
		switch command {
		case "MULTI":
			handleMulti(conn, tx)
		case "EXEC":
			// EXEC without MULTI
			resp.WriteError(conn, "ERR EXEC without MULTI")
		case "DISCARD":
			// DISCARD without MULTI
			resp.WriteError(conn, "ERR DISCARD without MULTI")
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
		case "SAVE":
			handleSave(conn, s)
		case "LPUSH":
			handleLPush(conn, s, args)
		case "RPUSH":
			handleRPush(conn, s, args)
		case "LPOP":
			handleLPop(conn, s, args)
		case "RPOP":
			handleRPop(conn, s, args)
		case "LRANGE":
			handleLRange(conn, s, args)
		case "LLEN":
			handleLLen(conn, s, args)
		case "SADD":
			handleSAdd(conn, s, args)
		case "SREM":
			handleSRem(conn, s, args)
		case "SMEMBERS":
			handleSMembers(conn, s, args)
		case "SISMEMBER":
			handleSIsMember(conn, s, args)
		case "HSET":
			handleHSet(conn, s, args)
		case "HGET":
			handleHGet(conn, s, args)
		case "HGETALL":
			handleHGetAll(conn, s, args)
		case "HDEL":
			handleHDel(conn, s, args)
		case "HEXISTS":
			handleHExists(conn, s, args)
		case "ZADD":
			handleZAdd(conn, s, args)
		case "ZSCORE":
			handleZScore(conn, s, args)
		case "ZRANK":
			handleZRank(conn, s, args)
		case "ZRANGE":
			handleZRange(conn, s, args)
		case "ZREVRANGE":
			handleZRevRange(conn, s, args)
		case "ZREM":
			handleZRem(conn, s, args)
		case "INCR":
			handleIncr(conn, s, args)
		case "DECR":
			handleDecr(conn, s, args)
		case "INCRBY":
			handleIncrBy(conn, s, args)
		case "KEYS":
			handleKeys(conn, s, args)
		case "SUBSCRIBE":
			handleSubscribe(conn, ps, args, clientID)
		case "PUBLISH":
			handlePublish(conn, ps, args)
		case "UNSUBSCRIBE":
			handleUnsubscribe(conn, ps, args, clientID)
		default:
			resp.WriteError(conn, fmt.Sprintf("unknown command '%s'", command))
		}
	}
}

// ─────────────────────────────────────────
//  TRANSACTION HANDLERS
// ─────────────────────────────────────────

// handleMulti starts a transaction
// Like picking up a shopping cart
func handleMulti(conn net.Conn, tx *Transaction) {
	tx.active = true
	tx.commands = [][]string{} // fresh empty queue
	resp.WriteSimpleString(conn, "OK")
}

// handleDiscard abandons the transaction
// Like putting the cart back without buying anything
func handleDiscard(conn net.Conn, tx *Transaction) {
	tx.active = false
	tx.commands = [][]string{}
	resp.WriteSimpleString(conn, "OK")
}

// handleExec executes all queued commands atomically
// Like going to checkout — all items processed at once
func handleExec(conn net.Conn, s *store.Store, ps *pubsub.PubSub, tx *Transaction, clientID string) {
	// Mark transaction as inactive FIRST
	tx.active = false

	// Get the queued commands and clear the queue
	commands := tx.commands
	tx.commands = [][]string{}

	// Write array header — we'll send one response per queued command
	// Like a receipt with one line per item
	conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(commands))))

	// Execute each queued command one by one
	for _, args := range commands {
		command := strings.ToUpper(args[0])

		// Execute the command normally
		// Same switch as above — just executed now instead of queued
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
		case "SAVE":
			handleSave(conn, s)
		case "LPUSH":
			handleLPush(conn, s, args)
		case "RPUSH":
			handleRPush(conn, s, args)
		case "LPOP":
			handleLPop(conn, s, args)
		case "RPOP":
			handleRPop(conn, s, args)
		case "LRANGE":
			handleLRange(conn, s, args)
		case "LLEN":
			handleLLen(conn, s, args)
		case "SADD":
			handleSAdd(conn, s, args)
		case "SREM":
			handleSRem(conn, s, args)
		case "SMEMBERS":
			handleSMembers(conn, s, args)
		case "SISMEMBER":
			handleSIsMember(conn, s, args)
		case "HSET":
			handleHSet(conn, s, args)
		case "HGET":
			handleHGet(conn, s, args)
		case "HGETALL":
			handleHGetAll(conn, s, args)
		case "HDEL":
			handleHDel(conn, s, args)
		case "HEXISTS":
			handleHExists(conn, s, args)
		case "ZADD":
			handleZAdd(conn, s, args)
		case "ZSCORE":
			handleZScore(conn, s, args)
		case "ZRANK":
			handleZRank(conn, s, args)
		case "ZRANGE":
			handleZRange(conn, s, args)
		case "ZREVRANGE":
			handleZRevRange(conn, s, args)
		case "ZREM":
			handleZRem(conn, s, args)
		case "INCR":
			handleIncr(conn, s, args)
		case "DECR":
			handleDecr(conn, s, args)
		case "DECRBY":
			handleDecrBy(conn, s, args)
		case "INCRBY":
			handleIncrBy(conn, s, args)
		case "KEYS":
			handleKeys(conn, s, args)
		default:
			resp.WriteError(conn, fmt.Sprintf("unknown command '%s'", command))
		}
	}
}

// ─────────────────────────────────────────
//  STRING HANDLERS
// ─────────────────────────────────────────

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

func handleExpire(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 3 {
		resp.WriteError(conn, "EXPIRE requires 2 arguments: key and seconds")
		return
	}
	seconds, err := strconv.Atoi(args[2])
	if err != nil {
		resp.WriteError(conn, "seconds must be a number")
		return
	}
	duration := time.Duration(seconds) * time.Second
	if s.Expire(args[1], duration) {
		conn.Write([]byte(":1\r\n"))
	} else {
		conn.Write([]byte(":0\r\n"))
	}
}

func handleTTL(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 2 {
		resp.WriteError(conn, "TTL requires 1 argument: key")
		return
	}
	ttl := s.TTL(args[1])
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", ttl)))
}

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
	s.SetEx(args[1], args[3], time.Duration(seconds)*time.Second)
	resp.WriteSimpleString(conn, "OK")
}

func handleSave(conn net.Conn, s *store.Store) {
	if err := s.Save(); err != nil {
		resp.WriteError(conn, "failed to save: "+err.Error())
		return
	}
	resp.WriteSimpleString(conn, "OK")
}

// ─────────────────────────────────────────
//  LIST HANDLERS
// ─────────────────────────────────────────

func handleLPush(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 3 {
		resp.WriteError(conn, "LPUSH requires key and at least 1 value")
		return
	}
	length := s.LPush(args[1], args[2:]...)
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", length)))
}

func handleRPush(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 3 {
		resp.WriteError(conn, "RPUSH requires key and at least 1 value")
		return
	}
	length := s.RPush(args[1], args[2:]...)
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", length)))
}

func handleLPop(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 2 {
		resp.WriteError(conn, "LPOP requires 1 argument: key")
		return
	}
	val, ok := s.LPop(args[1])
	if !ok {
		resp.WriteNull(conn)
		return
	}
	resp.WriteBulkString(conn, val)
}

func handleRPop(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 2 {
		resp.WriteError(conn, "RPOP requires 1 argument: key")
		return
	}
	val, ok := s.RPop(args[1])
	if !ok {
		resp.WriteNull(conn)
		return
	}
	resp.WriteBulkString(conn, val)
}

func handleLRange(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 4 {
		resp.WriteError(conn, "LRANGE requires key, start, stop")
		return
	}
	start, err1 := strconv.Atoi(args[2])
	stop, err2 := strconv.Atoi(args[3])
	if err1 != nil || err2 != nil {
		resp.WriteError(conn, "start and stop must be numbers")
		return
	}
	items := s.LRange(args[1], start, stop)
	conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(items))))
	for _, item := range items {
		resp.WriteBulkString(conn, item)
	}
}

func handleLLen(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 2 {
		resp.WriteError(conn, "LLEN requires 1 argument: key")
		return
	}
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", s.LLen(args[1]))))
}

// ─────────────────────────────────────────
//  SET HANDLERS
// ─────────────────────────────────────────

func handleSAdd(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 3 {
		resp.WriteError(conn, "SADD requires key and at least 1 member")
		return
	}
	added := s.SAdd(args[1], args[2:]...)
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", added)))
}

func handleSRem(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 3 {
		resp.WriteError(conn, "SREM requires key and at least 1 member")
		return
	}
	removed := s.SRem(args[1], args[2:]...)
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", removed)))
}

func handleSMembers(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 2 {
		resp.WriteError(conn, "SMEMBERS requires 1 argument: key")
		return
	}
	members := s.SMembers(args[1])
	conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(members))))
	for _, m := range members {
		resp.WriteBulkString(conn, m)
	}
}

func handleSIsMember(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 3 {
		resp.WriteError(conn, "SISMEMBER requires key and member")
		return
	}
	if s.SIsMember(args[1], args[2]) {
		conn.Write([]byte(":1\r\n"))
	} else {
		conn.Write([]byte(":0\r\n"))
	}
}

// ─────────────────────────────────────────
//  HASH HANDLERS
// ─────────────────────────────────────────

func handleHSet(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 4 {
		resp.WriteError(conn, "HSET requires key, field, value")
		return
	}
	s.HSet(args[1], args[2], args[3])
	resp.WriteSimpleString(conn, "OK")
}

func handleHGet(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 3 {
		resp.WriteError(conn, "HGET requires key and field")
		return
	}
	val, ok := s.HGet(args[1], args[2])
	if !ok {
		resp.WriteNull(conn)
		return
	}
	resp.WriteBulkString(conn, val)
}

func handleHGetAll(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 2 {
		resp.WriteError(conn, "HGETALL requires 1 argument: key")
		return
	}
	hash := s.HGetAll(args[1])
	conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(hash)*2)))
	for field, value := range hash {
		resp.WriteBulkString(conn, field)
		resp.WriteBulkString(conn, value)
	}
}

func handleHDel(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 3 {
		resp.WriteError(conn, "HDEL requires key and field")
		return
	}
	if s.HDel(args[1], args[2]) {
		conn.Write([]byte(":1\r\n"))
	} else {
		conn.Write([]byte(":0\r\n"))
	}
}

func handleHExists(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 3 {
		resp.WriteError(conn, "HEXISTS requires key and field")
		return
	}
	if s.HExists(args[1], args[2]) {
		conn.Write([]byte(":1\r\n"))
	} else {
		conn.Write([]byte(":0\r\n"))
	}
}

// ─────────────────────────────────────────
//  SORTED SET HANDLERS
// ─────────────────────────────────────────

func handleZAdd(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 4 {
		resp.WriteError(conn, "ZADD requires key, score, member")
		return
	}
	score, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		resp.WriteError(conn, "score must be a number")
		return
	}
	added := s.ZAdd(args[1], score, args[3])
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", added)))
}

func handleZScore(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 3 {
		resp.WriteError(conn, "ZSCORE requires key and member")
		return
	}
	score, ok := s.ZScore(args[1], args[2])
	if !ok {
		resp.WriteNull(conn)
		return
	}
	scoreStr := strconv.FormatFloat(score, 'f', -1, 64)
	resp.WriteBulkString(conn, scoreStr)
}

func handleZRank(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 3 {
		resp.WriteError(conn, "ZRANK requires key and member")
		return
	}
	rank, ok := s.ZRank(args[1], args[2])
	if !ok {
		resp.WriteNull(conn)
		return
	}
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", rank)))
}

func handleZRange(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 4 {
		resp.WriteError(conn, "ZRANGE requires key, start, stop")
		return
	}
	start, err1 := strconv.Atoi(args[2])
	stop, err2 := strconv.Atoi(args[3])
	if err1 != nil || err2 != nil {
		resp.WriteError(conn, "start and stop must be numbers")
		return
	}
	members := s.ZRange(args[1], start, stop)
	conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(members))))
	for _, m := range members {
		resp.WriteBulkString(conn, m)
	}
}

func handleZRevRange(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 4 {
		resp.WriteError(conn, "ZREVRANGE requires key, start, stop")
		return
	}
	start, err1 := strconv.Atoi(args[2])
	stop, err2 := strconv.Atoi(args[3])
	if err1 != nil || err2 != nil {
		resp.WriteError(conn, "start and stop must be numbers")
		return
	}
	members := s.ZRevRange(args[1], start, stop)
	conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(members))))
	for _, m := range members {
		resp.WriteBulkString(conn, m)
	}
}

func handleZRem(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 3 {
		resp.WriteError(conn, "ZREM requires key and member")
		return
	}
	if s.ZRem(args[1], args[2]) {
		conn.Write([]byte(":1\r\n"))
	} else {
		conn.Write([]byte(":0\r\n"))
	}
}

// ─────────────────────────────────────────
//  INCR / DECR HANDLERS
// ─────────────────────────────────────────

func handleIncr(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 2 {
		resp.WriteError(conn, "INCR requires 1 argument: key")
		return
	}
	val, err := s.Incr(args[1])
	if err != nil {
		resp.WriteError(conn, err.Error())
		return
	}
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", val)))
}

func handleDecr(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 2 {
		resp.WriteError(conn, "DECR requires 1 argument: key")
		return
	}
	val, err := s.Decr(args[1])
	if err != nil {
		resp.WriteError(conn, err.Error())
		return
	}
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", val)))
}

func handleIncrBy(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 3 {
		resp.WriteError(conn, "INCRBY requires key and amount")
		return
	}
	delta, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		resp.WriteError(conn, "amount must be a number")
		return
	}
	val, err := s.IncrBy(args[1], delta)
	if err != nil {
		resp.WriteError(conn, err.Error())
		return
	}
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", val)))
}

func handleDecrBy(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 3 {
		resp.WriteError(conn, "DECRBY requires key and amount")
		return
	}
	delta, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		resp.WriteError(conn, "amount must be a number")
		return
	}
	val, err := s.DecrBy(args[1], delta)
	if err != nil {
		resp.WriteError(conn, err.Error())
		return
	}
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", val)))
}

// ─────────────────────────────────────────
//  KEYS HANDLER
// ─────────────────────────────────────────

func handleKeys(conn net.Conn, s *store.Store, args []string) {
	if len(args) < 2 {
		resp.WriteError(conn, "KEYS requires a pattern")
		return
	}
	keys := s.Keys(args[1])
	conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(keys))))
	for _, k := range keys {
		resp.WriteBulkString(conn, k)
	}
}

// ─────────────────────────────────────────
//  PUB/SUB HANDLERS
// ─────────────────────────────────────────

func handleSubscribe(conn net.Conn, ps *pubsub.PubSub, args []string, clientID string) {
	if len(args) < 2 {
		resp.WriteError(conn, "SUBSCRIBE requires at least 1 channel")
		return
	}

	channels := args[1:]
	for _, channelName := range channels {
		sub := ps.Subscribe(clientID, channelName)
		writeSubscribeResponse(conn, "subscribe", channelName, 1)

		go func(ch chan string, channel string) {
			for msg := range ch {
				writeMessageResponse(conn, channel, msg)
			}
		}(sub.Channel, channelName)
	}
}

func handlePublish(conn net.Conn, ps *pubsub.PubSub, args []string) {
	if len(args) < 3 {
		resp.WriteError(conn, "PUBLISH requires channel and message")
		return
	}
	count := ps.Publish(args[1], args[2])
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", count)))
}

func handleUnsubscribe(conn net.Conn, ps *pubsub.PubSub, args []string, clientID string) {
	if len(args) < 2 {
		resp.WriteError(conn, "UNSUBSCRIBE requires at least 1 channel")
		return
	}
	for _, channelName := range args[1:] {
		ps.Unsubscribe(clientID, channelName)
		writeSubscribeResponse(conn, "unsubscribe", channelName, 0)
	}
}

func writeSubscribeResponse(conn net.Conn, kind, channel string, count int) {
	conn.Write([]byte("*3\r\n"))
	resp.WriteBulkString(conn, kind)
	resp.WriteBulkString(conn, channel)
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", count)))
}

func writeMessageResponse(conn net.Conn, channel, message string) {
	conn.Write([]byte("*3\r\n"))
	resp.WriteBulkString(conn, "message")
	resp.WriteBulkString(conn, channel)
	resp.WriteBulkString(conn, message)
}
