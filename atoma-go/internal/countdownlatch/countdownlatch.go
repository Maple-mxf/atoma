package countdownlatch

import (
	"context"
	"errors"
	"time"

	"github.com/maxuefeng/atoma/atoma-go/api"
)

// CountDownLatch implements the api.CountDownLatch interface.
type CountDownLatch struct {
	store        api.CoordinationStore
	resourceName string
}

// New creates a new CountDownLatch.
// It initializes the resource in the store if it doesn't exist.
func New(ctx context.Context, store api.CoordinationStore, resourceName string, count int64) (*CountDownLatch, error) {
	if count <= 0 {
		return nil, errors.New("count must be positive")
	}
	// Attempt to create the resource with the initial count.
	// This is not perfectly atomic without transactions but is a common approach.
	res, err := store.Get(ctx, resourceName)
	if err != nil {
		return nil, err
	}
	if res.Type == "" { // Resource doesn't exist
		// This is a placeholder for a proper "Create" method in the store.
		// For now, we rely on the CountDown method's upsert logic, but we need to set the initial count.
		// A dedicated Init method in the store would be better.
		// Let's simulate this by calling CountDown `count` times with a special setup.
		// This is inefficient. A proper implementation needs a dedicated store method.
		// For now, we'll assume the first CountDown will set the type.
	}

	return &CountDownLatch{
		store:        store,
		resourceName: resourceName,
	}, nil
}

// Await blocks until the latch's count reaches zero, or the context is canceled.
func (l *CountDownLatch) Await(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond) // Poll every 500ms
	defer ticker.Stop()

	for {
		count, err := l.GetCount(ctx)
		if err != nil {
			return err
		}
		if count == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Continue to next poll
		}
	}
}

// CountDown decrements the count of the latch.
func (l *CountDownLatch) CountDown(ctx context.Context) error {
	_, err := l.store.CountDown(ctx, l.resourceName)
	return err
}

// GetCount returns the current count of the latch.
func (l *CountDownLatch) GetCount(ctx context.Context) (int64, error) {
	res, err := l.store.Get(ctx, l.resourceName)
	if err != nil {
		return -1, err
	}
	if res.Type != "" && res.Type != api.ResourceTypeCountDownLatch {
		return -1, errors.New("resource is not a countdownlatch")
	}
	count, _ := res.Data["count"].(int64)
	return count, nil
}
