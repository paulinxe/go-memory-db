package db

import "sync"

const (
	// defaultNamespaceName is the pre-created namespace every registry starts with.
	defaultNamespaceName  = "default"
	maxNamespaceNameBytes = 64
)

// NamespaceRegistry maps logical namespace names to isolated Store instances.
type NamespaceRegistry struct {
	mutex  sync.Mutex // protects the modifications of the stores map
	stores map[string]*Store
}

// NewNamespaceRegistry returns a registry containing a default namespace with an empty Store.
func NewNamespaceRegistry() *NamespaceRegistry {
	r := &NamespaceRegistry{stores: make(map[string]*Store)}
	r.stores[defaultNamespaceName] = NewStore()
	return r
}

// CreateNamespace registers a new namespace with an empty Store. Idempotent: existing names unchanged.
// Name must be non-empty and at most maxNamespaceNameBytes UTF-8 bytes.
func (r *NamespaceRegistry) CreateNamespace(name string) error {
	if name == "" {
		return ErrInvalidNamespaceName
	}

	if len(name) > maxNamespaceNameBytes {
		return ErrNamespaceNameTooLong
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()
	if _, ok := r.stores[name]; ok {
		return nil
	}

	r.stores[name] = NewStore()
	return nil
}

// Get returns the store for name and whether it exists.
func (r *NamespaceRegistry) Get(name string) (*Store, bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	store, ok := r.stores[name]
	return store, ok
}

// GetDefault returns the store for the default namespace.
func (r *NamespaceRegistry) GetDefault() *Store {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.stores[defaultNamespaceName]
}
