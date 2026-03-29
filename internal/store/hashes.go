package store

// HSetOne sets a single field to value in the hash at key, creating the hash if needed.
func (s *Store) HSetOne(key, field, value string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.keyAlreadyExists(key, "hashes") {
		return ErrKeyAlreadyExists
	}

	if s.hashes[key] == nil {
		s.hashes[key] = make(map[string]string)
	}

	s.hashes[key][field] = value
	return nil
}

// HGetOne returns the value of field in the hash at key.
func (s *Store) HGetOne(key, field string) (string, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	hash, ok := s.hashes[key]
	if !ok {
		return "", ErrKeyNotFound
	}

	value, ok := hash[field]
	if !ok {
		return "", ErrFieldNotFound
	}

	return value, nil
}
