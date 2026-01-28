package api

import (
	"context"
	"time"
)

const (
	// ResourceTypeLock is a type identifier for a lock.
	ResourceTypeLock = "lock"
	// ResourceTypeReadWriteLock is a type identifier for a read-write lock.
	ResourceTypeReadWriteLock = "rwlock"
	// ResourceTypeSemaphore is a type identifier for a semaphore.
	ResourceTypeSemaphore = "semaphore"
	// ResourceTypeCountDownLatch is a type identifier for a countdown latch.
	ResourceTypeCountDownLatch = "countdownlatch"
)

// Resource represents a distributed resource that can be leased.
type Resource struct {
	// Name is the unique name of the resource.
	Name string
	// Type is the kind of resource (e.g., "lock", "semaphore").
	Type string
	// OwnerID is the identifier of the client that currently owns the resource.
	OwnerID string
	// Data holds type-specific information, e.g., number of permits for a semaphore.
	Data map[string]any
}

// CoordinationStore is the interface for backend storage systems
// that manage the state of distributed resources.
type CoordinationStore interface {
	// Acquire attempts to acquire a lease on a resource.
	Acquire(ctx context.Context, resource *Resource, leaseTime time.Duration) (*Lease, error)

	// Release gives up the ownership of a lease.
	Release(ctx context.Context, lease *Lease) error

	// Renew extends the duration of an existing lease.
	Renew(ctx context.Context, lease *Lease) error

	// Get retrieves the current state of a resource.
	Get(ctx context.Context, resourceName string) (*Resource, error)

	// CountDown decrements the count of a latch.
	CountDown(ctx context.Context, resourceName string) (int64, error)
}