package lease

import (
	"context"
	"log"
	"time"

	"github.com/maxuefeng/atoma/atoma-go/api"
)

// RenewalManager manages the automatic renewal of a lease in a background goroutine.
type RenewalManager struct {
	store api.CoordinationStore
}

// NewRenewalManager creates a new RenewalManager.
func NewRenewalManager(store api.CoordinationStore) *RenewalManager {
	return &RenewalManager{store: store}
}

// StartRenewal starts a background goroutine to periodically renew the given lease.
// The renewal stops when the provided context is canceled.
// It returns a channel that will receive an error if renewal fails.
func (rm *RenewalManager) StartRenewal(ctx context.Context, lease *api.Lease) <-chan error {
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)

		// Renew immediately once, then start a ticker.
		// This helps catch immediate issues.
		if err := rm.store.Renew(ctx, lease); err != nil {
			log.Printf("Initial renewal failed for lease %s: %v", lease.LeaseID, err)
			errChan <- err
			return
		}

		// Start ticker for periodic renewal. The interval should be significantly
		// shorter than the lease time to handle network delays gracefully.
		renewalInterval := lease.LeaseTime * 2 / 3
		if renewalInterval <= 0 {
			renewalInterval = time.Second // a sensible default for very short leases
		}
		ticker := time.NewTicker(renewalInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				// Context was canceled, so renewal should stop.
				log.Printf("Stopping renewal for lease %s as requested.", lease.LeaseID)
				return
			case <-ticker.C:
				if err := rm.store.Renew(ctx, lease); err != nil {
					log.Printf("Periodic renewal failed for lease %s: %v", lease.LeaseID, err)
					errChan <- err
					return
				}
				log.Printf("Successfully renewed lease %s for resource %s", lease.LeaseID, lease.ResourceName)
			}
		}
	}()

	return errChan
}
