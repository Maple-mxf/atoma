package api

import "time"

// Lease represents the ownership of a resource for a specific duration.
type Lease struct {
	// LeaseID is the unique identifier for this lease.
	LeaseID string
	// OwnerID is the identifier of the client that holds the lease.
	OwnerID string
	// ResourceName is the name of the resource this lease is for.
	ResourceName string
	// LeaseTime is the duration of the lease.
	LeaseTime time.Duration
	// ExpireAt is the time when the lease expires.
	ExpireAt time.Time
}
