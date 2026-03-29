package store

import "errors"

var ErrKeyAlreadyExists = errors.New("key already exists")
var ErrListEmpty = errors.New("list is empty")
var ErrKeyNotFound = errors.New("key not found")
var ErrFieldNotFound = errors.New("field not found")
var ErrInvalidHSetPairs = errors.New("hset pairs must have even length")
