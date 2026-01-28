package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/maxuefeng/atoma/atoma-go/api"
	"github.com/maxuefeng/atoma/atoma-go/client"
	"github.com/maxuefeng/atoma/atoma-go/storage/mongo"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// --- Configuration ---
const (
	mongoURI       = "mongodb://localhost:27017"
	dbName         = "atoma_db"
	collectionName = "resources"
)

func main() {
	log.Println("--- Atoma Examples ---")

	// --- 1. Setup MongoDB Connection & Atoma Client ---
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer mongoClient.Disconnect(context.Background())
	log.Println("Successfully connected to MongoDB.")

	store := mongo.NewMongoStore(mongoClient, dbName, collectionName)
	atomaClient := client.New(store)
	log.Println("Atoma client initialized.")

	// --- Run Examples ---
	runLockExample(atomaClient)
	runReadWriteLockExample(atomaClient)
	runCountDownLatchExample(atomaClient)

	log.Println("--- All Examples Finished ---")
}

func runLockExample(c *client.AtomaClient) {
	log.Println("\n--- Running Lock Example ---")
	lockName := "my-critical-task-lock"
	lock := c.NewLock(lockName)

	log.Printf("Attempting to acquire lock '%s'...", lockName)
	lease, err := lock.Acquire(context.Background(), 15*time.Second)
	if err != nil {
		log.Fatalf("Failed to acquire lock: %v", err)
	}
	log.Printf("Lock acquired! Lease ID: %s", lease.LeaseID)

	log.Println("Performing critical work...")
	time.Sleep(5 * time.Second)
	log.Println("Critical work finished.")

	log.Println("Releasing the lock...")
	if err := lock.Release(context.Background(), lease); err != nil {
		log.Fatalf("Failed to release lock: %v", err)
	}
	log.Println("Lock released successfully.")
	log.Println("--- Lock Example Finished ---")
}

func runReadWriteLockExample(c *client.AtomaClient) {
	log.Println("\n--- Running ReadWriteLock Example ---")
	rwLockName := "my-rw-lock"
	rwLock := c.NewReadWriteLock(rwLockName)

	// Acquire a read lock
	log.Println("Acquiring read lock...")
	readLease, err := rwLock.ReadLock().Acquire(context.Background(), 20*time.Second)
	if err != nil {
		log.Fatalf("Failed to acquire read lock: %v", err)
	}
	log.Println("Read lock acquired.")

	// Try to acquire a write lock in a separate goroutine (this should block)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("(Goroutine) Attempting to acquire write lock (should be blocked)...")
		writeLock := c.NewReadWriteLock(rwLockName).WriteLock()
		// Use a context with timeout to prevent blocking forever
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := writeLock.Acquire(ctx, 20*time.Second)
		if err != nil {
			log.Printf("(Goroutine) Correctly failed to acquire write lock while read lock is held: %v", err)
		} else {
			log.Fatal("(Goroutine) Incorrectly acquired write lock!")
		}
	}()

	time.Sleep(2 * time.Second) // Give the goroutine time to try

	log.Println("Releasing read lock...")
	if err := rwLock.ReadLock().Release(context.Background(), readLease); err != nil {
		log.Fatalf("Failed to release read lock: %v", err)
	}
	log.Println("Read lock released.")

	wg.Wait() // Wait for the goroutine to finish its attempt

	// Now, acquire a write lock
	log.Println("Acquiring write lock...")
	writeLease, err := rwLock.WriteLock().Acquire(context.Background(), 20*time.Second)
	if err != nil {
		log.Fatalf("Failed to acquire write lock: %v", err)
	}
	log.Println("Write lock acquired.")
	log.Println("Releasing write lock...")
	if err := rwLock.WriteLock().Release(context.Background(), writeLease); err != nil {
		log.Fatalf("Failed to release write lock: %v", err)
	}
	log.Println("Write lock released.")
	log.Println("--- ReadWriteLock Example Finished ---")
}

func runCountDownLatchExample(c *client.AtomaClient) {
	log.Println("\n--- Running CountDownLatch Example ---")
	latchName := "my-countdown-latch"
	latch, err := c.NewCountDownLatch(context.Background(), latchName, 3)
	if err != nil {
		log.Fatalf("Failed to create countdown latch: %v", err)
	}

	// In a real scenario, you might need to reset the latch in the DB manually for re-running the example.
	// This example assumes a clean state.

	var wg sync.WaitGroup
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()
			log.Printf("(Worker %d) Performing work...", workerNum)
			time.Sleep(time.Duration(workerNum) * time.Second)
			log.Printf("(Worker %d) Work done, counting down.", workerNum)
			if err := latch.CountDown(context.Background()); err != nil {
				log.Printf("(Worker %d) Error counting down: %v", workerNum, err)
			}
		}(i)
	}

	log.Println("(Main) Awaiting countdown...")
	awaitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := latch.Await(awaitCtx); err != nil {
		log.Fatalf("Error awaiting latch: %v", err)
	}

	log.Println("(Main) Latch opened, all workers finished!")
	wg.Wait() // Ensure all goroutines have finished logging
	log.Println("--- CountDownLatch Example Finished ---")
}