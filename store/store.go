package store

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// entry holds a value and its optional expiry time
type entry struct {
	Value     string    `json:"value"`
	ExpiresAt time.Time `json:"expires_at"`
	HasExpiry bool      `json:"has_expiry"`
}

// isExpired checks if this entry has passed its expiry time
func (e *entry) isExpired() bool {
	if !e.HasExpiry {
		return false
	}
	return time.Now().After(e.ExpiresAt)
}

// Store is our in-memory key-value store with persistence
type Store struct {
	mu       sync.RWMutex
	data     map[string]*entry
	filePath string // where we save the snapshot
}

// NewStore creates a new store and loads data from disk if it exists
func NewStore(filePath string) *Store {
	s := &Store{
		data:     make(map[string]*entry),
		filePath: filePath,
	}

	// Load existing data from disk when server starts
	// Like restoring from the last photograph
	if err := s.load(); err != nil {
		fmt.Println("No existing data found, starting fresh")
	} else {
		fmt.Printf("Loaded data from %s\n", filePath)
	}

	// Start background janitor — cleans expired keys every second
	go s.cleanupExpiredKeys()

	// Start background saver — saves snapshot every 60 seconds
	go s.autoSave()

	return s
}

// Set stores a key-value pair with no expiry
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = &entry{Value: value}
}

// SetEx stores a key-value pair with an expiry duration
func (s *Store) SetEx(key, value string, duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = &entry{
		Value:     value,
		ExpiresAt: time.Now().Add(duration),
		HasExpiry: true,
	}
}

// Get retrieves a value
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok || e.isExpired() {
		return "", false
	}
	return e.Value, true
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

// Exists checks if a key exists and hasn't expired
func (s *Store) Exists(key string) bool {
	_, ok := s.Get(key)
	return ok
}

// Expire sets expiry on an existing key
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

// TTL returns remaining seconds for a key
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

// Persist removes expiry from a key
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

// Save writes the entire store to disk as JSON
// Like taking a photograph of the whiteboard
func (s *Store) Save() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Convert our map to JSON bytes
	// Like: objectMapper.writeValueAsString(map) in Java
	bytes, err := json.Marshal(s.data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	// Write JSON bytes to file
	// os.WriteFile creates the file if it doesn't exist
	err = os.WriteFile(s.filePath, bytes, 0644)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	fmt.Printf("💾 Saved %d keys to %s\n", len(s.data), s.filePath)
	return nil
}

// load reads data from disk into memory
// Like restoring from the last photograph
func (s *Store) load() error {
	// Read the file from disk
	bytes, err := os.ReadFile(s.filePath)
	if err != nil {
		return err // File doesn't exist yet — that's OK
	}

	// Convert JSON bytes back into our map
	// Like: objectMapper.readValue(json, Map.class) in Java
	var data map[string]*entry
	if err := json.Unmarshal(bytes, &data); err != nil {
		return fmt.Errorf("failed to unmarshal data: %w", err)
	}

	s.data = data
	return nil
}

// autoSave runs in the background, saving every 60 seconds
func (s *Store) autoSave() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if err := s.Save(); err != nil {
			fmt.Println("Auto-save failed:", err)
		}
	}
}

// cleanupExpiredKeys runs every second and removes expired keys
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