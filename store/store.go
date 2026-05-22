package store

import "sync"

// Store is our in-memory key-value store
// Think of it like a thread-safe HashMap in Java
type Store struct {
	mu   sync.RWMutex      // The lock — controls concurrent access
	data map[string]string  // The actual whiteboard — key/value pairs
}

// NewStore creates a new empty store
// Like: new HashMap<>() in Java
func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Set stores a key-value pair
// Like: hashMap.put("name", "John")
func (s *Store) Set(key, value string) {
	s.mu.Lock()         // Lock the whiteboard — nobody else can write or read
	defer s.mu.Unlock() // When done, unlock automatically

	s.data[key] = value
}

// Get retrieves a value by key
// Like: hashMap.get("name")
// Returns the value AND a bool (true if found, false if not)
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()         // Read lock — others can read too, but nobody can write
	defer s.mu.RUnlock() // Unlock when done

	val, ok := s.data[key]
	return val, ok
}

// Del deletes a key
// Like: hashMap.remove("name")
// Returns true if key existed, false if it didn't
func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	return ok
}

// Exists checks if a key exists
// Like: hashMap.containsKey("name")
func (s *Store) Exists(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.data[key]
	return ok
}