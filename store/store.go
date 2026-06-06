package store

import (
	"sync"
	"time"
)

// entry holds a value and its optional expiry time
// Like a sticky note with an optional "throw away after X" instruction
type entry struct {
	value     string
	expiresAt time.Time
	hasExpiry bool
}

// isExpired checks if this entry has passed its expiry time
func (e *entry) isExpired() bool {
	if !e.hasExpiry {
		return false // No expiry set — lives forever
	}
	return time.Now().After(e.expiresAt)
}

// Store is our in-memory key-value store
type Store struct {
	mu   sync.RWMutex
	data map[string]*entry
}

// NewStore creates a new empty store
func NewStore() *Store {
	s := &Store{
		data: make(map[string]*entry),
	}
	// Start background goroutine to clean up expired keys
	go s.cleanupExpiredKeys()
	return s
}

// Set stores a key-value pair with no expiry
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = &entry{value: value}
}

// SetEx stores a key-value pair with an expiry duration
// Like: SET name John + EXPIRE name 10 in one shot
func (s *Store) SetEx(key, value string, duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = &entry{
		value:     value,
		expiresAt: time.Now().Add(duration),
		hasExpiry: true,
	}
}

// Get retrieves a value — returns empty string and false if not found or expired
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok {
		return "", false
	}

	// Check if the key has expired
	// Like checking if the sticky note's time has passed
	if e.isExpired() {
		return "", false
	}

	return e.value, true
}

// Del deletes a key
func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	return ok
}

// Exists checks if a key exists and is not expired
func (s *Store) Exists(key string) bool {
	_, ok := s.Get(key)
	return ok
}

// Expire sets an expiry duration on an existing key
// Returns true if key exists, false if it doesn't
func (s *Store) Expire(key string, duration time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok {
		return false
	}

	// Update the expiry on the existing entry
	e.expiresAt = time.Now().Add(duration)
	e.hasExpiry = true
	return true
}

// TTL returns how many seconds remain before the key expires
// Returns:
//   -1 if key exists but has no expiry
//   -2 if key doesn't exist or is already expired
//    N if N seconds remain
func (s *Store) TTL(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.isExpired() {
		return -2 // Key doesn't exist
	}

	if !e.hasExpiry {
		return -1 // Key exists but never expires
	}

	// Calculate remaining seconds
	remaining := time.Until(e.expiresAt)
	return int(remaining.Seconds())
}

// Persist removes the expiry from a key — makes it live forever
func (s *Store) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok {
		return false
	}

	e.hasExpiry = false
	return true
}

// cleanupExpiredKeys runs in the background every second
// and removes keys that have expired
// Like a janitor who walks around every second erasing old sticky notes
func (s *Store) cleanupExpiredKeys() {
	// time.NewTicker is like a clock that ticks every 1 second
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