package store

import "errors"

var ErrKeyAlreadyExists = errors.New("key already exists")
var ErrListEmpty = errors.New("list is empty")
