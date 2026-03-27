package server

import "sync"

// Store is the in-memory store for the database.
type Store struct {
	mutex   sync.RWMutex
	strings map[string]string
}

func NewStore() *Store {
	return &Store{
		strings: make(map[string]string),
	}
}

func (s *Store) Set(key, value string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.strings[key] = value
}

func (s *Store) Get(key string) (string, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	value, ok := s.strings[key]
	return value, ok
}

func (s *Store) Del(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.strings, key)
}

func (s *Store) Keys() []string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// We make a copy as we don't want to return the underlying map
	// that is protected by the mutex
	keys := make([]string, 0, len(s.strings))
	for key := range s.strings {
		keys = append(keys, key)
	}

	return keys
}
