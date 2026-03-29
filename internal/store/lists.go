package store

// LPush appends a single value to the list at key.
func (s *Store) LPush(key, value string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.keyAlreadyExists(key, "lists") {
		return ErrKeyAlreadyExists
	}

	s.lists[key] = append(s.lists[key], value)
	return nil
}

// TODO: we will need a LPushMultiple
// TODO: add comments on each function

func (s *Store) LPop(key string) (string, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if len(s.lists[key]) == 0 {
		return "", ErrListEmpty
	}

	value := s.lists[key][len(s.lists[key])-1]
	s.lists[key] = s.lists[key][:len(s.lists[key])-1]
	return value, nil
}

// LGet returns a copy of lists[key]
func (s *Store) LGet(key string) []string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	out := make([]string, len(s.lists[key]))
	copy(out, s.lists[key])
	return out
}
