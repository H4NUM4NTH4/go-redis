package store

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

type entry struct {
	Value     string             `json:"value,omitempty"`
	ListVal   []string           `json:"list_val,omitempty"`
	SetVal    map[string]bool    `json:"set_val,omitempty"`
	HashVal   map[string]string  `json:"hash_val,omitempty"`
	ZSetVal   map[string]float64 `json:"zset_val,omitempty"`
	Type      string             `json:"type"`
	ExpiresAt time.Time          `json:"expires_at"`
	HasExpiry bool               `json:"has_expiry"`
}

func (e *entry) isExpired() bool {
	if !e.HasExpiry {
		return false
	}
	return time.Now().After(e.ExpiresAt)
}

type Store struct {
	mu       sync.RWMutex
	data     map[string]*entry
	filePath string
}

func NewStore(filePath string) *Store {
	s := &Store{
		data:     make(map[string]*entry),
		filePath: filePath,
	}

	if err := s.load(); err != nil {
		fmt.Println("No existing data found, starting fresh")
	} else {
		fmt.Printf("Loaded data from %s\n", filePath)
	}

	go s.cleanupExpiredKeys()
	go s.autoSave()

	return s
}

// ─────────────────────────────────────────
//  STRING COMMANDS
// ─────────────────────────────────────────

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = &entry{Type: "string", Value: value}
}

func (s *Store) SetEx(key, value string, duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = &entry{
		Type:      "string",
		Value:     value,
		ExpiresAt: time.Now().Add(duration),
		HasExpiry: true,
	}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.isExpired() || e.Type != "string" {
		return "", false
	}
	return e.Value, true
}

func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	return ok
}

func (s *Store) Exists(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	return ok && !e.isExpired()
}

func (s *Store) Expire(key string, duration time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok {
		return false
	}
	e.ExpiresAt = time.Now().Add(duration)
	e.HasExpiry = true
	return true
}

func (s *Store) TTL(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.isExpired() {
		return -2
	}
	if !e.HasExpiry {
		return -1
	}
	return int(time.Until(e.ExpiresAt).Seconds())
}

func (s *Store) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok {
		return false
	}
	e.HasExpiry = false
	return true
}

// ─────────────────────────────────────────
//  LIST COMMANDS
// ─────────────────────────────────────────

func (s *Store) LPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok {
		e = &entry{Type: "list", ListVal: []string{}}
		s.data[key] = e
	}

	for _, v := range values {
		e.ListVal = append([]string{v}, e.ListVal...)
	}
	return len(e.ListVal)
}

func (s *Store) RPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok {
		e = &entry{Type: "list", ListVal: []string{}}
		s.data[key] = e
	}

	e.ListVal = append(e.ListVal, values...)
	return len(e.ListVal)
}

func (s *Store) LPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok || len(e.ListVal) == 0 {
		return "", false
	}

	val := e.ListVal[0]
	e.ListVal = e.ListVal[1:]
	return val, true
}

func (s *Store) RPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok || len(e.ListVal) == 0 {
		return "", false
	}

	n := len(e.ListVal)
	val := e.ListVal[n-1]
	e.ListVal = e.ListVal[:n-1]
	return val, true
}

func (s *Store) LRange(key string, start, stop int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.Type != "list" {
		return []string{}
	}

	n := len(e.ListVal)

	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop {
		return []string{}
	}

	return e.ListVal[start : stop+1]
}

func (s *Store) LLen(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.Type != "list" {
		return 0
	}
	return len(e.ListVal)
}

// ─────────────────────────────────────────
//  SET COMMANDS
// ─────────────────────────────────────────

func (s *Store) SAdd(key string, members ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok {
		e = &entry{Type: "set", SetVal: make(map[string]bool)}
		s.data[key] = e
	}

	added := 0
	for _, m := range members {
		if !e.SetVal[m] {
			e.SetVal[m] = true
			added++
		}
	}
	return added
}

func (s *Store) SRem(key string, members ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok || e.Type != "set" {
		return 0
	}

	removed := 0
	for _, m := range members {
		if e.SetVal[m] {
			delete(e.SetVal, m)
			removed++
		}
	}
	return removed
}

func (s *Store) SMembers(key string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.Type != "set" {
		return []string{}
	}

	members := make([]string, 0, len(e.SetVal))
	for m := range e.SetVal {
		members = append(members, m)
	}
	return members
}

func (s *Store) SIsMember(key, member string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.Type != "set" {
		return false
	}
	return e.SetVal[member]
}

// ─────────────────────────────────────────
//  HASH COMMANDS
// ─────────────────────────────────────────

func (s *Store) HSet(key, field, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok {
		e = &entry{Type: "hash", HashVal: make(map[string]string)}
		s.data[key] = e
	}
	e.HashVal[field] = value
}

func (s *Store) HGet(key, field string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.Type != "hash" {
		return "", false
	}
	val, ok := e.HashVal[field]
	return val, ok
}

func (s *Store) HGetAll(key string) map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.Type != "hash" {
		return map[string]string{}
	}
	return e.HashVal
}

func (s *Store) HDel(key, field string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok || e.Type != "hash" {
		return false
	}
	_, ok = e.HashVal[field]
	if ok {
		delete(e.HashVal, field)
	}
	return ok
}

func (s *Store) HExists(key, field string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.Type != "hash" {
		return false
	}
	_, ok = e.HashVal[field]
	return ok
}

// ─────────────────────────────────────────
//  SORTED SET COMMANDS
// ─────────────────────────────────────────

func (s *Store) ZAdd(key string, score float64, member string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok {
		e = &entry{Type: "zset", ZSetVal: make(map[string]float64)}
		s.data[key] = e
	}

	_, exists := e.ZSetVal[member]
	e.ZSetVal[member] = score

	if exists {
		return 0
	}
	return 1
}

func (s *Store) ZScore(key, member string) (float64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.Type != "zset" {
		return 0, false
	}

	score, ok := e.ZSetVal[member]
	return score, ok
}

func (s *Store) ZRank(key, member string) (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.Type != "zset" {
		return 0, false
	}

	sorted := getSortedMembers(e.ZSetVal)

	for i, m := range sorted {
		if m == member {
			return i, true
		}
	}
	return 0, false
}

func (s *Store) ZRange(key string, start, stop int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.Type != "zset" {
		return []string{}
	}

	sorted := getSortedMembers(e.ZSetVal)
	return sliceRange(sorted, start, stop)
}

func (s *Store) ZRevRange(key string, start, stop int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.Type != "zset" {
		return []string{}
	}

	sorted := getSortedMembers(e.ZSetVal)

	for i, j := 0, len(sorted)-1; i < j; i, j = i+1, j-1 {
		sorted[i], sorted[j] = sorted[j], sorted[i]
	}

	return sliceRange(sorted, start, stop)
}

func (s *Store) ZRem(key, member string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok || e.Type != "zset" {
		return false
	}

	_, exists := e.ZSetVal[member]
	if exists {
		delete(e.ZSetVal, member)
	}
	return exists
}

func getSortedMembers(zset map[string]float64) []string {
	members := make([]string, 0, len(zset))
	for m := range zset {
		members = append(members, m)
	}

	sort.Slice(members, func(i, j int) bool {
		return zset[members[i]] < zset[members[j]]
	})

	return members
}

func sliceRange(items []string, start, stop int) []string {
	n := len(items)
	if n == 0 {
		return []string{}
	}

	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop {
		return []string{}
	}

	return items[start : stop+1]
}

// ─────────────────────────────────────────
//  INCR / DECR COMMANDS
// ─────────────────────────────────────────

func (s *Store) Incr(key string) (int64, error) {
	return s.IncrBy(key, 1)
}

func (s *Store) Decr(key string) (int64, error) {
	return s.IncrBy(key, -1)
}

func (s *Store) DecrBy(key string, delta int64) (int64, error) {
    return s.IncrBy(key, -delta)
}

func (s *Store) IncrBy(key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]

	var current int64 = 0

	if ok {
		val, err := strconv.ParseInt(e.Value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("value is not an integer")
		}
		current = val
	}

	current += delta

	s.data[key] = &entry{
		Type:  "string",
		Value: strconv.FormatInt(current, 10),
	}

	return current, nil
}

// ─────────────────────────────────────────
//  KEYS COMMAND
// ─────────────────────────────────────────

func (s *Store) Keys(pattern string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	matches := []string{}

	for key, e := range s.data {
		if e.isExpired() {
			continue
		}
		if matchPattern(pattern, key) {
			matches = append(matches, key)
		}
	}

	return matches
}

func matchPattern(pattern, key string) bool {
	if pattern == "*" {
		return true
	}
	return globMatch(pattern, key)
}

func globMatch(pattern, str string) bool {
	if len(pattern) == 0 && len(str) == 0 {
		return true
	}

	if pattern == "*" {
		return true
	}

	if len(pattern) == 0 {
		return false
	}

	if len(str) == 0 {
		return pattern == "*"
	}

	p := pattern[0]

	if p == '*' {
		return globMatch(pattern[1:], str) ||
			globMatch(pattern, str[1:])
	} else if p == '?' {
		return globMatch(pattern[1:], str[1:])
	} else {
		if p != str[0] {
			return false
		}
		return globMatch(pattern[1:], str[1:])
	}
}

// ─────────────────────────────────────────
//  PERSISTENCE
// ─────────────────────────────────────────

func (s *Store) Save() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bytes, err := json.Marshal(s.data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	err = os.WriteFile(s.filePath, bytes, 0644)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	fmt.Printf("💾 Saved %d keys to %s\n", len(s.data), s.filePath)
	return nil
}

func (s *Store) load() error {
	bytes, err := os.ReadFile(s.filePath)
	if err != nil {
		return err
	}

	var data map[string]*entry
	if err := json.Unmarshal(bytes, &data); err != nil {
		return fmt.Errorf("failed to unmarshal data: %w", err)
	}

	s.data = data
	return nil
}

func (s *Store) autoSave() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if err := s.Save(); err != nil {
			fmt.Println("Auto-save failed:", err)
		}
	}
}

func (s *Store) cleanupExpiredKeys() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.mu.Lock()
		for key, e := range s.data {
			if e.isExpired() {
				delete(s.data, key)
			}
		}
		s.mu.Unlock()
	}
}