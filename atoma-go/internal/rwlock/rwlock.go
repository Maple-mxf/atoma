package rwlock

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/maxuefeng/atoma/atoma-go/api"
	"github.com/maxuefeng/atoma/atoma-go/internal/lease"
	"github.com/google/uuid"
)

// rwLock implements the api.Lock interface for either read or write mode.
type rwLock struct {
	isReadLock     bool
	store          api.CoordinationStore
	renewalManager *lease.RenewalManager
	resourceName   string
	ownerID        string
	activeLeases   map[string]context.CancelFunc
	mu             sync.Mutex
}

// ReadWriteLock implements the api.ReadWriteLock interface.
type ReadWriteLock struct {
	readLock  api.Lock
	writeLock api.Lock
}

// New creates a new ReadWriteLock.
func New(store api.CoordinationStore, resourceName string) *ReadWriteLock {
	ownerID := uuid.New().String() // Shared ownerID for both read and write locks of this instance
	renewalManager := lease.NewRenewalManager(store)
	activeLeases := make(map[string]context.CancelFunc)

	return &ReadWriteLock{
		readLock: &rwLock{
			isReadLock:     true,
			store:          store,
			renewalManager: renewalManager,
			resourceName:   resourceName,
			ownerID:        ownerID,
			activeLeases:   activeLeases,
		},
		writeLock: &rwLock{
			isReadLock:     false,
			store:          store,
			renewalManager: renewalManager,
			resourceName:   resourceName,
			ownerID:        ownerID,
			activeLeases:   activeLeases,
		},
	}
}

func (l *ReadWriteLock) ReadLock() api.Lock {
	return l.readLock
}

func (l *ReadWriteLock) WriteLock() api.Lock {
	return l.writeLock
}

// --- rwLock implementation of api.Lock ---

func (l *rwLock) Acquire(ctx context.Context, leaseTime time.Duration) (*api.Lease, error) {
	// Simple blocking retry loop
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		lease, err := l.TryAcquire(ctx, leaseTime)
		if err == nil {
			return lease, nil
		}
		// A more specific error would be better here.
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			// Continue retrying on generic lock-held errors
		} else {
			return nil, err // Propagate context errors
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (l *rwLock) TryAcquire(ctx context.Context, leaseTime time.Duration) (*api.Lease, error) {
	resource := &api.Resource{
		Name:    l.resourceName,
		Type:    api.ResourceTypeReadWriteLock,
		OwnerID: l.ownerID,
		Data:    map[string]any{"isReadLock": l.isReadLock},
	}

	lease, err := l.store.Acquire(ctx, resource, leaseTime)
	if err != nil {
		return nil, err
	}

	l.startLeaseManagement(lease)
	return lease, nil
}

func (l *rwLock) Release(ctx context.Context, lease *api.Lease) error {
	l.mu.Lock()
	cancel, ok := l.activeLeases[lease.LeaseID]
	if !ok {
		l.mu.Unlock()
		return errors.New("lease not found or not active")
	}
	cancel()
	delete(l.activeLeases, lease.LeaseID)
	l.mu.Unlock()

	// The store's Release method needs to know if it's a read or write lock.
	// We can add this info to the lease object, or the store can fetch it.
	// The current store.Release fetches it, so this is fine.
	return l.store.Release(ctx, lease)
}

func (l *rwLock) startLeaseManagement(lease *api.Lease) {
	l.mu.Lock()
	defer l.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	l.activeLeases[lease.LeaseID] = cancel

	errChan := l.renewalManager.StartRenewal(ctx, lease)
	go func() {
		err := <-errChan
		if err != nil {
			l.mu.Lock()
			delete(l.activeLeases, lease.LeaseID)
			l.mu.Unlock()
		}
	}()
}
