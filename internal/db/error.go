package db

import "errors"

var ErrKeyAlreadyExists = errors.New("key already exists")
var ErrListEmpty = errors.New("list is empty")
var ErrKeyNotFound = errors.New("key not found")
var ErrFieldNotFound = errors.New("field not found")
var ErrInvalidHSetPairs = errors.New("hset pairs must have even length")
var ErrInvalidNamespaceName = errors.New("invalid namespace name")
var ErrNamespaceNameTooLong = errors.New("namespace name too long")
var ErrNamespaceDoesNotExist = errors.New("namespace does not exist")
var ErrCannotDeleteDefaultNamespace = errors.New("cannot delete default namespace")
var ErrInvalidInteger = errors.New("invalid integer")
var ErrExpiryTooLow = errors.New("expiry too low")
