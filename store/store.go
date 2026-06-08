package store

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// entry holds any type of value with optional expiry
type entry struct {
	Value     string            `json:"value,omitempty"`
	ListVal   []string          `json:"list_val,omitempty"`
	SetVal    map[string]bool   `json:"set_val,omitempty"`
	HashVal   map[string]string `json:"hash_val,omitempty"`
	Type      string            `json:"type"` // "string", "list", "set", "hash"
	ExpiresAt time.Time         `json:"expires_at"`
	HasExpiry bool              `json:"has_expiry"`
}

// isExpired checks if this entry has passed its expiry time
func (e *entry) isExpired() bool {
	if !e.HasExpiry {
		return false
	}
	return time.Now().After(e.ExpiresAt)
}

// Store is our in-memory key-value store
type Store struct {
	mu       sync.RWMutex
	data     map[string]*entry
	filePath string
}

// NewStore creates a new store and loads from disk
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

// LPush adds values to the LEFT (front) of the list
// Like adding to the front of a queue
func (s *Store) LPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok {
		// Key doesn't exist — create a new list
		e = &entry{Type: "list", ListVal: []string{}}
		s.data[key] = e
	}

	// Add each value to the FRONT of the slice
	// Like cutting in line at the front
	for _, v := range values {
		e.ListVal = append([]string{v}, e.ListVal...)
	}
	return len(e.ListVal)
}

// RPush adds values to the RIGHT (back) of the list
func (s *Store) RPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok {
		e = &entry{Type: "list", ListVal: []string{}}
		s.data[key] = e
	}

	// Add each value to the BACK of the slice
	e.ListVal = append(e.ListVal, values...)
	return len(e.ListVal)
}

// LPop removes and returns the LEFT (front) element
func (s *Store) LPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok || len(e.ListVal) == 0 {
		return "", false
	}

	// Take the first element
	val := e.ListVal[0]
	// Remove it from the slice
	e.ListVal = e.ListVal[1:]
	return val, true
}

// RPop removes and returns the RIGHT (back) element
func (s *Store) RPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok || len(e.ListVal) == 0 {
		return "", false
	}

	// Take the last element
	n := len(e.ListVal)
	val := e.ListVal[n-1]
	// Remove it from the slice
	e.ListVal = e.ListVal[:n-1]
	return val, true
}

// LRange returns elements from index start to stop
// Like slicing a list — LRange key 0 -1 means "give me everything"
func (s *Store) LRange(key string, start, stop int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.Type != "list" {
		return []string{}
	}

	n := len(e.ListVal)

	// Handle negative indexes — "-1" means last element
	// Like Python's list[-1]
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}

	// Boundary checks
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

// LLen returns the length of a list
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

// SAdd adds members to a set — duplicates ignored
func (s *Store) SAdd(key string, members ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok {
		e = &entry{Type: "set", SetVal: make(map[string]bool)}
		s.data[key] = e
	}

	// Count how many NEW members we added
	added := 0
	for _, m := range members {
		if !e.SetVal[m] {
			e.SetVal[m] = true
			added++
		}
	}
	return added
}

// SRem removes members from a set
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

// SMembers returns all members of a set
func (s *Store) SMembers(key string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.Type != "set" {
		return []string{}
	}

	// Convert map keys to slice
	members := make([]string, 0, len(e.SetVal))
	for m := range e.SetVal {
		members = append(members, m)
	}
	return members
}

// SIsMember checks if a value is in the set
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

// HSet sets a field in a hash
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

// HGet gets a field from a hash
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

// HGetAll returns all fields and values in a hash
func (s *Store) HGetAll(key string) map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.Type != "hash" {
		return map[string]string{}
	}
	return e.HashVal
}

// HDel deletes a field from a hash
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

// HExists checks if a field exists in a hash
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