package store

func (s *Store) Set(key, value string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.keyAlreadyExists(key, "strings") {
		return ErrKeyAlreadyExists
	}

	s.strings[key] = value
	return nil
}

func (s *Store) Get(key string) (string, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	value, ok := s.strings[key]
	return value, ok
}
