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
