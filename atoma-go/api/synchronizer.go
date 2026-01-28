package api

import (
	"context"
	"time"
)

// Lock provides a distributed mutual exclusion lock.
type Lock interface {
	Acquire(ctx context.Context, leaseTime time.Duration) (*Lease, error)
	TryAcquire(ctx context.Context, leaseTime time.Duration) (*Lease, error)
	Release(ctx context.Context, lease *Lease) error
}

// ReadWriteLock provides a distributed reader-writer lock.
type ReadWriteLock interface {
	ReadLock() Lock
	WriteLock() Lock
}

// Semaphore provides a distributed counting semaphore.
type Semaphore interface {
	Acquire(ctx context.Context, n int, leaseTime time.Duration) (*Lease, error)
	TryAcquire(ctx context.Context, n int, leaseTime time.Duration) (*Lease, error)
	Release(ctx context.Context, lease *Lease) error
}

// CountDownLatch provides a distributed synchronization aid that allows one or more
// threads to wait until a set of operations being performed in other threads completes.
type CountDownLatch interface {
	Await(ctx context.Context) error
	CountDown(ctx context.Context) error
	GetCount(ctx context.Context) (int64, error)
}