package store

import "sync"

// Store is the in-memory store for the database.
type Store struct {
	mutex   sync.RWMutex
	strings map[string]string
	lists   map[string][]string
}

func NewStore() *Store {
	return &Store{
		strings: make(map[string]string),
		lists:   make(map[string][]string),
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

	return false
}
