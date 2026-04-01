package db

import (
	"context"
	"sync"
)

const (
	// defaultNamespaceName is the pre-created namespace every registry starts with.
	defaultNamespaceName  = "default"
	maxNamespaceNameBytes = 64
)

// Namespace is a logical database. It owns a Store plus a lifecycle context that is
// cancelled when the namespace is deleted.
type Namespace struct {
	store  *Store
	ctx    context.Context
	cancel context.CancelFunc
}

func newNamespace() *Namespace {
	ctx, cancel := context.WithCancel(context.Background())
	namespace := &Namespace{
		store:  NewStore(),
		ctx:    ctx,
		cancel: cancel, // Called when a namespace is deleted.
	}

	namespace.store.startExpiryDaemon(ctx)
	return namespace
}

// GetStore returns the underlying store.
func (n *Namespace) GetStore() *Store {
	return n.store
}

// IsDeleted returns a channel that is closed when the namespace is deleted.
func (n *Namespace) IsDeleted() <-chan struct{} {
	return n.ctx.Done()
}

// NamespaceRegistry maps logical namespace names to isolated Store instances.
type NamespaceRegistry struct {
	mutex      sync.RWMutex
	namespaces map[string]*Namespace
}

// NewNamespaceRegistry returns a registry containing a default namespace with an empty Store.
func NewNamespaceRegistry() *NamespaceRegistry {
	r := &NamespaceRegistry{namespaces: make(map[string]*Namespace)}
	r.namespaces[defaultNamespaceName] = newNamespace()
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
	if _, ok := r.namespaces[name]; ok {
		return nil
	}

	r.namespaces[name] = newNamespace()
	return nil
}

// Get returns the namespace for name and whether it exists.
func (r *NamespaceRegistry) Get(name string) (*Namespace, bool) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	namespace, ok := r.namespaces[name]
	return namespace, ok
}

// GetDefault returns the namespace for the default namespace.
func (r *NamespaceRegistry) GetDefault() *Namespace {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.namespaces[defaultNamespaceName]
}

// DeleteNamespace deletes a namespace from the registry and cancels it so attached sessions
// immediately become invalid.
func (r *NamespaceRegistry) DeleteNamespace(name string) error {
	if name == defaultNamespaceName {
		return ErrCannotDeleteDefaultNamespace
	}

	r.mutex.Lock()
	namespace, ok := r.namespaces[name]
	if !ok {
		r.mutex.Unlock()
		return ErrNamespaceDoesNotExist
	}

	delete(r.namespaces, name)
	r.mutex.Unlock()

	// By cancelling the context, all attached sessions working on this namespace will get an error when trying
	// to send commands to the namespace.
	namespace.cancel()
	return nil
}

// Shutdown cancels all namespaces (including default). Intended for server shutdown / tests.
func (r *NamespaceRegistry) Shutdown() {
	// We copy the namespaces to a slice so we can unlock the mutex before cancelling the contexts.
	// This is to avoid deadlocks as cancelling is synchronous and could block the mutex.
	r.mutex.Lock()
	namespaces := make([]*Namespace, 0, len(r.namespaces))
	for _, ns := range r.namespaces {
		namespaces = append(namespaces, ns)
	}
	r.mutex.Unlock()

	for _, ns := range namespaces {
		ns.cancel()
	}
}
