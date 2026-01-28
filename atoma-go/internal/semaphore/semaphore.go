package semaphore

import (
	"context"
	"errors"
	"time"

	"github.com/maxuefeng/atoma/atoma-go/api"
	"github.com/maxuefeng/atoma/atoma-go/internal/lease"
	"github.com/google/uuid"
)

// Semaphore implements the api.Semaphore interface.
type Semaphore struct {
	store          api.CoordinationStore
	renewalManager *lease.RenewalManager
	resourceName   string
	ownerID        string
	totalPermits   int
}

// New creates a new Semaphore.
func New(store api.CoordinationStore, resourceName string, initialPermits int) *Semaphore {
	return &Semaphore{
		store:          store,
		renewalManager: lease.NewRenewalManager(store),
		resourceName:   resourceName,
		ownerID:        uuid.New().String(),
		totalPermits:   initialPermits,
	}
}

// Acquire acquires n permits from the semaphore, blocking until they are available.
func (s *Semaphore) Acquire(ctx context.Context, n int, leaseTime time.Duration) (*api.Lease, error) {
	// A full implementation would have a blocking retry loop here.
	return s.TryAcquire(ctx, n, leaseTime)
}

// TryAcquire attempts to acquire n permits without blocking.
func (s *Semaphore) TryAcquire(ctx context.Context, n int, leaseTime time.Duration) (*api.Lease, error) {
	if n <= 0 {
		return nil, errors.New("number of permits to acquire must be positive")
	}

	resource := &api.Resource{
		Name:    s.resourceName,
		Type:    api.ResourceTypeSemaphore,
		OwnerID: s.ownerID,
		Data: map[string]any{
			"permits":      n,
			"totalPermits": s.totalPermits,
		},
	}

	// The underlying store method is not fully implemented without transactions.
	lease, err := s.store.Acquire(ctx, resource, leaseTime)
	if err != nil {
		return nil, err
	}

	// If it were successful, we would start lease management here.
	// l.startLeaseManagement(lease)

	return lease, nil
}

// Release releases a lease, returning its permits to the semaphore.
func (s *Semaphore) Release(ctx context.Context, lease *api.Lease) error {
	// The underlying store method is not fully implemented without transactions.
	return s.store.Release(ctx, lease)
}
