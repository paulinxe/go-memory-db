package db

import "sort"

// HSet merges alternating field/value tokens into the hash at key, creating the hash if needed.
// pairs must have even length (field1, value1, field2, value2, …).
func (s *Store) HSet(key string, pairs []string) error {
	if len(pairs) % 2 != 0 {
		return ErrInvalidHSetPairs
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.keyAlreadyExists(key, "hashes") {
		return ErrKeyAlreadyExists
	}

	if s.hashes[key] == nil {
		s.hashes[key] = make(map[string]string)
	}

	for i := 0; i < len(pairs); i += 2 {
		s.hashes[key][pairs[i]] = pairs[i+1]
	}

	return nil
}

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

// HGet returns all field-value pairs for key as a flat slice [field1, value1, ...] sorted by field name.
// If there is no hash at key or it is empty, it returns nil (same empty wire reply as LGET).
func (s *Store) HGet(key string) []string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	hash, ok := s.hashes[key]
	if !ok || len(hash) == 0 {
		return nil
	}

	keys := make([]string, 0, len(hash))
	for k := range hash {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]string, 0, len(keys)*2)
	for _, k := range keys {
		out = append(out, k, hash[k])
	}

	return out
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

// HDel removes field from the hash at key. If the hash becomes empty, the key is removed from hashes.
// If there is no hash at key, it succeeds as a no-op (same key may exist as a string or list).
func (s *Store) HDel(key, field string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	hash, ok := s.hashes[key]
	if !ok {
		return
	}

	delete(hash, field)
	if len(hash) == 0 {
		delete(s.hashes, key)
	}
}
