package lock

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/maxuefeng/atoma/atoma-go/api"
	"github.com/maxuefeng/atoma/atoma-go/internal/lease"
	"github.com/google/uuid"
)

var (
	// ErrLockHeld is returned by TryAcquire when the lock is already held by another client.
	ErrLockHeld = errors.New("lock is held by another client")
)

// Mutex implements the api.Lock interface.
type Mutex struct {
	store          api.CoordinationStore
	renewalManager *lease.RenewalManager
	resourceName   string
	ownerID        string

	// activeLeases tracks the cancel functions for renewal goroutines.
	// Key: LeaseID
	activeLeases   map[string]context.CancelFunc
	mu             sync.Mutex
}

// NewMutex creates a new Mutex for a given resource.
func NewMutex(store api.CoordinationStore, resourceName string) *Mutex {
	return &Mutex{
		store:          store,
		renewalManager: lease.NewRenewalManager(store),
		resourceName:   resourceName,
		ownerID:        uuid.New().String(), // Each mutex instance gets a unique owner ID
		activeLeases:   make(map[string]context.CancelFunc),
	}
}

// Acquire obtains the lock, blocking until it is available.
func (m *Mutex) Acquire(ctx context.Context, leaseTime time.Duration) (*api.Lease, error) {
	// Simple blocking retry loop. A more advanced implementation could use a watch mechanism if the store supports it.
	ticker := time.NewTicker(1 * time.Second) // Retry every second
	defer ticker.Stop()

	for {
		lease, err := m.TryAcquire(ctx, leaseTime)
		if err == nil {
			return lease, nil // Success
		}
		if !errors.Is(err, ErrLockHeld) {
			return nil, err // A real error occurred
		}

		// Lock is held, wait for the next tick or context cancellation.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			// Continue to next iteration
		}
	}
}

// TryAcquire attempts to obtain the lock without blocking.
func (m *Mutex) TryAcquire(ctx context.Context, leaseTime time.Duration) (*api.Lease, error) {
	resource := &api.Resource{
		Name:    m.resourceName,
		OwnerID: m.ownerID,
	}

	lease, err := m.store.Acquire(ctx, resource, leaseTime)
	if err != nil {
		// Translate the generic error from the store to a more specific one for the lock API.
		return nil, ErrLockHeld
	}

	// If acquisition is successful, start the renewal process.
	m.startLeaseManagement(lease)

	return lease, nil
}

// Release relinquishes the lock.
func (m *Mutex) Release(ctx context.Context, lease *api.Lease) error {
	m.mu.Lock()
	cancel, ok := m.activeLeases[lease.LeaseID]
	if !ok {
		m.mu.Unlock()
		return errors.New("lease not found or not active")
	}
	// Stop the renewal goroutine.
	cancel()
	delete(m.activeLeases, lease.LeaseID)
	m.mu.Unlock()

	// Release the lock from the backend store.
	return m.store.Release(ctx, lease)
}

func (m *Mutex) startLeaseManagement(lease *api.Lease) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create a context that can be canceled by the Release method.
	ctx, cancel := context.WithCancel(context.Background())
	m.activeLeases[lease.LeaseID] = cancel

	// Start the renewal manager and listen for errors.
	errChan := m.renewalManager.StartRenewal(ctx, lease)
	go func() {
		err := <-errChan
		if err != nil {
			// If renewal fails, the lease is lost. We should clean up.
			// A more advanced implementation might have a callback to notify the client.
			m.mu.Lock()
			delete(m.activeLeases, lease.LeaseID)
			m.mu.Unlock()
		}
	}()
}
