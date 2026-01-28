package client

import (
	"context"

	"github.com/maxuefeng/atoma/atoma-go/api"
	"github.com/maxuefeng/atoma/atoma-go/internal/countdownlatch"
	"github.com/maxuefeng/atoma/atoma-go/internal/lock"
	"github.com/maxuefeng/atoma/atoma-go/internal/rwlock"
	"github.com/maxuefeng/atoma/atoma-go/internal/semaphore"
)

// AtomaClient is the main entry point for interacting with the Atoma distributed primitives.
type AtomaClient struct {
	store api.CoordinationStore
}

// New creates a new AtomaClient.
func New(store api.CoordinationStore) *AtomaClient {
	return &AtomaClient{
		store: store,
	}
}

// NewLock creates a new distributed lock for a given resource name.
func (c *AtomaClient) NewLock(resourceName string) api.Lock {
	return lock.NewMutex(c.store, resourceName)
}

// NewReadWriteLock creates a new distributed reader-writer lock.
func (c *AtomaClient) NewReadWriteLock(resourceName string) api.ReadWriteLock {
	return rwlock.New(c.store, resourceName)
}

// NewSemaphore creates a new distributed counting semaphore.
// Note: The current backend implementation for Semaphore is a stub and not fully atomic.
func (c *AtomaClient) NewSemaphore(resourceName string, initialPermits int) api.Semaphore {
	return semaphore.New(c.store, resourceName, initialPermits)
}

// NewCountDownLatch creates a new distributed countdown latch.
func (c *AtomaClient) NewCountDownLatch(ctx context.Context, resourceName string, count int64) (api.CountDownLatch, error) {
	return countdownlatch.New(ctx, c.store, resourceName, count)
}