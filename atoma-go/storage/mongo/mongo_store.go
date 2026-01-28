package mongo

import (
	"context"
	"errors"
	"time"

	"github.com/maxuefeng/atoma/atoma-go/api"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// LeaseInfo is the BSON representation of lease details.
type LeaseInfo struct {
	LeaseID  string    `bson:"leaseId"`
	OwnerID  string    `bson:"ownerId"`
	ExpireAt time.Time `bson:"expireAt"`
	Permits  int       `bson:"permits,omitempty"` // For semaphore
}

// bsonResource is the generic BSON representation of a distributed resource.
type bsonResource struct {
	Name         string       `bson:"_id"`
	Type         string       `bson:"type"`
	WriteOwner   *LeaseInfo   `bson:"writeOwner,omitempty"`
	ReadOwners   []*LeaseInfo `bson:"readOwners,omitempty"`
	TotalPermits int          `bson:"totalPermits,omitempty"`
	Owners       []*LeaseInfo `bson:"owners,omitempty"`
	Count        int64        `bson:"count,omitempty"`
}

// MongoStore implements the api.CoordinationStore interface using MongoDB.
type MongoStore struct {
	collection *mongo.Collection
}

// NewMongoStore creates a new MongoStore.
func NewMongoStore(client *mongo.Client, dbName, collectionName string) *MongoStore {
	return &MongoStore{
		collection: client.Database(dbName).Collection(collectionName),
	}
}

// Acquire dispatches the acquisition logic based on the resource type.
func (s *MongoStore) Acquire(ctx context.Context, resource *api.Resource, leaseTime time.Duration) (*api.Lease, error) {
	switch resource.Type {
	case api.ResourceTypeLock:
		return s.acquireWriteLock(ctx, resource, leaseTime) // A simple lock is a write lock
	case api.ResourceTypeReadWriteLock:
		isReadLock, _ := resource.Data["isReadLock"].(bool)
		if isReadLock {
			return s.acquireReadLock(ctx, resource, leaseTime)
		}
		return s.acquireWriteLock(ctx, resource, leaseTime)
	case api.ResourceTypeSemaphore:
		permits, _ := resource.Data["permits"].(int)
		if permits <= 0 {
			return nil, errors.New("number of permits to acquire must be positive")
		}
		return s.acquireSemaphore(ctx, resource, leaseTime, permits)
	default:
		return nil, errors.New("unsupported resource type for acquire")
	}
}

// Release dispatches the release logic based on the resource type.
func (s *MongoStore) Release(ctx context.Context, lease *api.Lease) error {
	// To determine the type, we must fetch the resource first.
	res, err := s.Get(ctx, lease.ResourceName)
	if err != nil {
		return err
	}

	switch res.Type {
	case api.ResourceTypeLock:
		return s.releaseWriteLock(ctx, lease)
	case api.ResourceTypeReadWriteLock:
		isReadLock, _ := res.Data["isReadLock"].(bool)
		if isReadLock {
			return s.releaseReadLock(ctx, lease)
		}
		return s.releaseWriteLock(ctx, lease)
	case api.ResourceTypeSemaphore:
		return s.releaseSemaphore(ctx, lease)
	default:
		return errors.New("unsupported resource type for release")
	}
}

// Renew dispatches the renewal logic.
func (s *MongoStore) Renew(ctx context.Context, lease *api.Lease) error {
	// Renewal logic is similar for all lease types. We try to find the lease
	// in any of the possible owner fields/arrays and update its expiry.
	now := time.Now()
	newExpireAt := now.Add(lease.LeaseTime)

	filter := bson.M{
		"_id": lease.ResourceName,
		"$or": []bson.M{
			{"writeOwner.leaseId": lease.LeaseID, "writeOwner.ownerId": lease.OwnerID},
			{"readOwners": bson.M{"$elemMatch": bson.M{"leaseId": lease.LeaseID, "ownerId": lease.OwnerID}}},
			{"owners": bson.M{"$elemMatch": bson.M{"leaseId": lease.LeaseID, "ownerId": lease.OwnerID}}},
		},
	}

	// This update is complex because the lease could be in one of three places.
	// A single query to update an element in one of several arrays is not straightforward.
	// We will use a simpler, if less efficient, read-modify-write within a transaction,
	// or just focus on the most common cases. For now, let's handle them separately.
	// A more robust solution might require transactions.

	// Simplified renewal for write locks
	res, err := s.collection.UpdateOne(ctx,
		bson.M{"_id": lease.ResourceName, "writeOwner.leaseId": lease.LeaseID},
		bson.M{"$set": bson.M{"writeOwner.expireAt": newExpireAt}},
	)
	if err == nil && res.MatchedCount > 0 {
		lease.ExpireAt = newExpireAt
		return nil
	}

	// Simplified renewal for read locks
	res, err = s.collection.UpdateOne(ctx,
		bson.M{"_id": lease.ResourceName, "readOwners.leaseId": lease.LeaseID},
		bson.M{"$set": bson.M{"readOwners.$.expireAt": newExpireAt}},
	)
	if err == nil && res.MatchedCount > 0 {
		lease.ExpireAt = newExpireAt
		return nil
	}

	// Simplified renewal for semaphores
	res, err = s.collection.UpdateOne(ctx,
		bson.M{"_id": lease.ResourceName, "owners.leaseId": lease.LeaseID},
		bson.M{"$set": bson.M{"owners.$.expireAt": newExpireAt}},
	)
	if err == nil && res.MatchedCount > 0 {
		lease.ExpireAt = newExpireAt
		return nil
	}

	return errors.New("failed to renew lease: not found or not owned")
}

// Get retrieves the current state of a resource.
func (s *MongoStore) Get(ctx context.Context, resourceName string) (*api.Resource, error) {
	var storedRes bsonResource
	err := s.collection.FindOne(ctx, bson.M{"_id": resourceName}).Decode(&storedRes)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return &api.Resource{Name: resourceName}, nil // Return an empty resource
		}
		return nil, err
	}

	res := &api.Resource{
		Name: storedRes.Name,
		Type: storedRes.Type,
		Data: make(map[string]any),
	}
	// Populate data based on type
	switch storedRes.Type {
	case api.ResourceTypeSemaphore:
		res.Data["totalPermits"] = storedRes.TotalPermits
		res.Data["ownerCount"] = len(storedRes.Owners)
	case api.ResourceTypeCountDownLatch:
		res.Data["count"] = storedRes.Count
	case api.ResourceTypeReadWriteLock:
		res.Data["isWriteLocked"] = storedRes.WriteOwner != nil
		res.Data["readLockCount"] = len(storedRes.ReadOwners)
	}

	return res, nil
}

// CountDown decrements the count of a latch.
func (s *MongoStore) CountDown(ctx context.Context, resourceName string) (int64, error) {
	filter := bson.M{"_id": resourceName, "count": bson.M{"$gt": 0}}
	update := bson.M{"$inc": bson.M{"count": -1}}
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After).SetUpsert(true)

	var updatedDoc bsonResource
	err := s.collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&updatedDoc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			// This means the count was already 0.
			return 0, nil
		}
		return -1, err
	}
	return updatedDoc.Count, nil
}

// --- Internal acquisition methods ---

func (s *MongoStore) newLeaseInfo(ownerID string, leaseTime time.Duration, permits int) (*LeaseInfo, time.Time) {
	now := time.Now()
	expireAt := now.Add(leaseTime)
	return &LeaseInfo{
		LeaseID:  uuid.New().String(),
		OwnerID:  ownerID,
		ExpireAt: expireAt,
		Permits:  permits,
	}, now
}

func (s *MongoStore) acquireWriteLock(ctx context.Context, resource *api.Resource, leaseTime time.Duration) (*api.Lease, error) {
	leaseInfo, now := s.newLeaseInfo(resource.OwnerID, leaseTime, 0)

	filter := bson.M{
		"_id": resource.Name,
		"$or": []bson.M{
			{"writeOwner": bson.M{"$exists": false}},
			{"writeOwner": nil},
			{"writeOwner.expireAt": bson.M{"$lt": now}},
		},
		"readOwners.0": bson.M{"$exists": false}, // readOwners is empty
	}
	update := bson.M{
		"$set": bson.M{
			"writeOwner": leaseInfo,
			"type":       resource.Type,
		},
	}
	opts := options.UpdateOptions{Upsert: new(bool)}; *opts.Upsert = true

	res, err := s.collection.UpdateOne(ctx, filter, update, &opts)
	if err != nil {
		return nil, err
	}
	if res.MatchedCount == 0 && res.UpsertedCount == 0 {
		return nil, errors.New("lock is held by another client")
	}

	return &api.Lease{
		LeaseID: leaseInfo.LeaseID, OwnerID: leaseInfo.OwnerID, ResourceName: resource.Name,
		LeaseTime: leaseTime, ExpireAt: leaseInfo.ExpireAt,
	}, nil
}

func (s *MongoStore) acquireReadLock(ctx context.Context, resource *api.Resource, leaseTime time.Duration) (*api.Lease, error) {
	leaseInfo, now := s.newLeaseInfo(resource.OwnerID, leaseTime, 0)

	filter := bson.M{
		"_id": resource.Name,
		"$or": []bson.M{
			{"writeOwner": bson.M{"$exists": false}},
			{"writeOwner": nil},
			{"writeOwner.expireAt": bson.M{"$lt": now}},
		},
	}
	update := bson.M{
		"$push": bson.M{"readOwners": leaseInfo},
		"$setOnInsert": bson.M{"type": resource.Type},
	}
	opts := options.UpdateOptions{Upsert: new(bool)}; *opts.Upsert = true

	res, err := s.collection.UpdateOne(ctx, filter, update, &opts)
	if err != nil {
		return nil, err
	}
	if res.MatchedCount == 0 && res.UpsertedCount == 0 {
		return nil, errors.New("lock is held by a writer")
	}

	return &api.Lease{
		LeaseID: leaseInfo.LeaseID, OwnerID: leaseInfo.OwnerID, ResourceName: resource.Name,
		LeaseTime: leaseTime, ExpireAt: leaseInfo.ExpireAt,
	}, nil
}

func (s *MongoStore) acquireSemaphore(ctx context.Context, resource *api.Resource, leaseTime time.Duration, permits int) (*api.Lease, error) {
	leaseInfo, _ := s.newLeaseInfo(resource.OwnerID, leaseTime, permits)
	totalPermits := resource.Data["totalPermits"].(int)

	// This is complex. A simple model is to have an "availablePermits" field.
	filter := bson.M{
		"_id": resource.Name,
		"availablePermits": bson.M{"$gte": permits},
	}
	update := bson.M{
		"$inc":         bson.M{"availablePermits": -permits},
		"$push":        bson.M{"owners": leaseInfo},
		"$setOnInsert": bson.M{"type": resource.Type, "totalPermits": totalPermits},
	}
	// This doesn't work on upsert. A transaction or a more complex setup is needed.
	// For this implementation, we assume the semaphore is created beforehand.
	// A simplified approach for now:
	return nil, errors.New("semaphore acquisition not fully implemented in store")
}


// --- Internal release methods ---

func (s *MongoStore) releaseWriteLock(ctx context.Context, lease *api.Lease) error {
	filter := bson.M{"_id": lease.ResourceName, "writeOwner.leaseId": lease.LeaseID}
	update := bson.M{"$unset": bson.M{"writeOwner": ""}}
	res, err := s.collection.UpdateOne(ctx, filter, update)
	if err == nil && res.MatchedCount == 0 {
		return errors.New("write lock not found or not owned")
	}
	return err
}

func (s *MongoStore) releaseReadLock(ctx context.Context, lease *api.Lease) error {
	filter := bson.M{"_id": lease.ResourceName}
	update := bson.M{"$pull": bson.M{"readOwners": bson.M{"leaseId": lease.LeaseID}}}
	res, err := s.collection.UpdateOne(ctx, filter, update)
	if err == nil && res.ModifiedCount == 0 {
		return errors.New("read lock not found or not owned")
	}
	return err
}

func (s *MongoStore) releaseSemaphore(ctx context.Context, lease *api.Lease) error {
	// Needs to find the lease in the owners array, get the number of permits,
	// and then increment availablePermits and pull the lease. Requires a transaction.
	return errors.New("semaphore release not fully implemented in store")
}