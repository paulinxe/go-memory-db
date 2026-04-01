package db

import (
	"sync"
	"time"
)

// Store is the in-memory store for the database.
type Store struct {
	mutex   sync.RWMutex
	strings map[string]string
	lists   map[string][]string
	hashes  map[string]map[string]string
	expiry  map[string]time.Time
}

func NewStore() *Store {
	return &Store{
		strings: make(map[string]string),
		lists:   make(map[string][]string),
		hashes:  make(map[string]map[string]string),
		expiry:  make(map[string]time.Time),
	}
}

// keyAlreadyExists There is no need to use a lock here as the caller MUST hold the lock.
func (s *Store) keyAlreadyExists(key string, excludeType string) bool {
	if _, ok := s.strings[key]; ok && excludeType != "strings" {
		return true
	}

	if _, ok := s.lists[key]; ok && excludeType != "lists" {
		return true
	}

	if _, ok := s.hashes[key]; ok && excludeType != "hashes" {
		return true
	}

	return false
}

func (s *Store) Del(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.strings, key)
	delete(s.lists, key)
	delete(s.hashes, key)
	delete(s.expiry, key)
}

func (s *Store) Keys() []string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	keys := make([]string, 0, len(s.strings)+len(s.lists)+len(s.hashes))
	for key := range s.strings {
		keys = append(keys, key)
	}
	for key := range s.lists {
		keys = append(keys, key)
	}
	for key := range s.hashes {
		keys = append(keys, key)
	}

	return keys
}
