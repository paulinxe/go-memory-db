package db

import (
	"context"
	"strconv"
	"time"
)

// Expire implements the EXPIRE command semantics (including parsing).
func (s *Store) Expire(key, secondsToken string) (error) {
	seconds, err := strconv.Atoi(secondsToken)
	if err != nil {
		return ErrInvalidInteger
	}

	if seconds <= 0 {
		return ErrExpiryTooLow
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !s.keyAlreadyExists(key, "") {
		return ErrKeyNotFound
	}

	s.expiry[key] = time.Now().Add(time.Duration(seconds) * time.Second)
	return nil
}

// TTL returns the remaining seconds for key based solely on the expiry map.
// It returns -1 if key has no entry in the expiry map.
func (s *Store) TTL(key string) string {
	s.mutex.RLock()
	expiresAt, exists := s.expiry[key]
	s.mutex.RUnlock()
	if !exists {
		return "-1"
	}

	remaining := int(time.Until(expiresAt).Seconds())
	if remaining < 0 {
		// This could happen if the sweeper has not run yet.
		return "0"
	}

	return strconv.Itoa(remaining)
}

func (s *Store) startExpiryDaemon(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.purgeExpired()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// purgeExpired must use an exclusive lock because:
// - it mutates maps (delete from values + expiry metadata)
// - doing RLock → unlock → Lock introduces a TOCTOU race condition
func (s *Store) purgeExpired() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	now := time.Now()
	for key, expiresAt := range s.expiry {
		if now.After(expiresAt) {
			delete(s.strings, key)
			delete(s.lists, key)
			delete(s.hashes, key)
			delete(s.expiry, key)
		}
	}
}
