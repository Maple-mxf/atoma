package atoma.client;

import static org.junit.jupiter.api.Assertions.*;

import atoma.api.Lease;
import atoma.api.coordination.CoordinationStore;
import atoma.api.lock.Lock;
import atoma.core.internal.lock.DefaultMutexLock;
import atoma.storage.mongo.MongoCoordinationStore;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Flow test for {@link DefaultMutexLock}.
 *
 * <p><b>Note:</b> This test requires a local MongoDB instance running on {@code localhost:27017}.
 */
public class DefaultMutexLockFlowTest {

  private MongoClient mongoClient;
  private CoordinationStore coordinationStore;
  private String resourceId;

  private AtomaClient atomaClient;
  private Lease lease;

  @BeforeEach
  void setUp() {
    // This test connects to a real MongoDB instance.
    // Make sure you have a local MongoDB server running on the default port.
    try {
      mongoClient = MongoClients.create("mongodb://localhost:27017/atoma_benchmark");
      coordinationStore = new MongoCoordinationStore(mongoClient, "atoma_benchmark");

      atomaClient = new AtomaClient(coordinationStore);
      lease = atomaClient.grantLease(Duration.ofSeconds(30));

      resourceId = "mutex-lock-flow-test-" + UUID.randomUUID();
    } catch (Exception e) {
      // Fail fast if mongo is not available
      throw new IllegalStateException(
          "Failed to connect to local MongoDB. Please ensure it is running on localhost:27017.", e);
    }
  }

  @AfterEach
  void tearDown() throws Exception {
    lease.close();
    atomaClient.close();

    if (mongoClient != null) {
      try {
        // Clean up the test database
        mongoClient.getDatabase("atoma-test-db").drop();
      } finally {
        mongoClient.close();
      }
    }
  }

  @Test
  @Timeout(10) // Fails the test if it takes more than 10 seconds, preventing infinite blocking.
  void testLockUnlockFlow_secondThreadWaitsForFirst() throws Exception {
    Lock lock = lease.getLock(resourceId);
    ExecutorService executor = Executors.newFixedThreadPool(2);

    // A shared resource to verify exclusive access
    List<String> accessOrder = new ArrayList<>();

    CountDownLatch thread1AcquiredLock = new CountDownLatch(1);
    CountDownLatch thread2AttemptedLock = new CountDownLatch(1);
    CountDownLatch thread1ReleasedLock = new CountDownLatch(1);

    // Thread 1: Acquires the lock first
    Future<?> future1 =
        executor.submit(
            () -> {
              try {
                lock.lock();
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              try {
                accessOrder.add("thread-1");
                thread1AcquiredLock.countDown();
                // Hold the lock and wait for thread 2 to attempt to acquire it
                try {
                  thread2AttemptedLock.await(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              } finally {
                lock.unlock();
                System.out.println("F1 Unlock success");
                thread1ReleasedLock.countDown();
              }
            });

    // Thread 2: Attempts to acquire the lock while thread 1 holds it
    Future<?> future2 =
        executor.submit(
            () -> {
              try {
                // Wait until thread 1 has the lock
                thread1AcquiredLock.await(3, TimeUnit.SECONDS);

                // Attempt to acquire the lock again, this time it should succeed
                lock.lock();
                try {
                  accessOrder.add("thread-2");
                } finally {
                  lock.unlock();
                  System.out.println("F2 Unlock success");
                }

              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });

    // Wait for both threads to complete
    executor.shutdown();
    assertTrue(
        executor.awaitTermination(5, TimeUnit.SECONDS), "Executor did not terminate in time");

    // Verify the futures to catch any exceptions
    try {
      future1.get();
      future2.get();
    } catch (ExecutionException e) {
      fail("One of the test threads threw an exception", e);
    }

    // Assert the access order
    assertEquals(2, accessOrder.size());
    assertEquals("thread-1", accessOrder.get(0));
    assertEquals("thread-2", accessOrder.get(1));

    lock.close();
  }

  @Test
  void testTryLock_whenLockIsFree_acquiresSuccessfully() throws Exception {
    Lock lock = lease.getLock(resourceId);
    assertTrue(lock.tryLock(), "Should acquire lock successfully when it's free");
    try {
      // Lock is held, further assertions can be made here if needed
    } finally {
      lock.unlock();
    }
    lock.close();
  }

  @Test
  void testTryLock_whenLockIsHeld_failsImmediately() throws Exception {
    Lock lock1 = lease.getLock(resourceId);
    Lock lock2;

    // Use a different lease to simulate a different client
    try (Lease separateLease = atomaClient.grantLease(Duration.ofSeconds(10))) {
      lock2 = separateLease.getLock(resourceId);

      // Client 1 acquires the lock
      assertTrue(lock1.tryLock(), "Lock 1 should be acquired");

      // Client 2 attempts to acquire the same lock
      assertFalse(lock2.tryLock(), "Lock 2 should fail to acquire the lock immediately");
    } finally {
      lock1.unlock();
    }

    lock1.close();
  }

  @Test
  @Timeout(5)
  void testTryLockWithTimeout_whenLockIsReleasedWithinTimeout_acquiresSuccessfully()
      throws Exception {
    Lock lock1 = lease.getLock(resourceId);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    // Acquire the lock in the main thread
    lock1.lock();

    // In a separate thread, try to acquire the lock with a timeout
    Future<Boolean> future =
        executor.submit(
            () -> {
              try (Lease separateLease = atomaClient.grantLease(Duration.ofSeconds(10))) {
                Lock lock2 = separateLease.getLock(resourceId);
                // Wait for 2 seconds, which is less than the main thread's hold time
                return lock2.tryLock(3, TimeUnit.SECONDS);
              }
            });

    // Wait a moment, then release the lock, well within the timeout of the other thread
    Thread.sleep(1000);
    lock1.unlock();

    // The other thread should have acquired the lock
    assertTrue(future.get(), "Should have acquired the lock within the timeout");

    executor.shutdown();
    assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
    lock1.close();
  }

  @Test
  @Timeout(5)
  void testTryLockWithTimeout_whenLockIsNotReleasedWithinTimeout_fails() throws Exception {
    Lock lock1 = lease.getLock(resourceId);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    // Acquire and hold the lock in the main thread
    lock1.lock();

    // In a separate thread, try to acquire the lock with a short timeout
    Future<Boolean> future =
        executor.submit(
            () -> {
              try (Lease separateLease = atomaClient.grantLease(Duration.ofSeconds(10))) {
                Lock lock2 = separateLease.getLock(resourceId);
                // This timeout is expected to expire
                return lock2.tryLock(1, TimeUnit.SECONDS);
              }
            });

    // Wait for the timeout to pass and verify the attempt failed
    assertFalse(future.get(), "Should have failed to acquire the lock after the timeout");

    // Cleanup
    lock1.unlock();
    executor.shutdown();
    assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
    lock1.close();
  }

  @Test
  @Timeout(5)
  void testLockInterruptibly_whenInterrupted_throwsInterruptedException() throws Exception {
    Lock lock = lease.getLock(resourceId);

    // Acquire the lock in the main thread to create contention
    lock.lock();

    CountDownLatch threadStartedWaiting = new CountDownLatch(1);
    ExecutorService executor = Executors.newSingleThreadExecutor();

    Future<?> future =
        executor.submit(
            () -> {
              Thread.currentThread().setName("Interruptible-Thread");
              try (Lease separateLease = atomaClient.grantLease(Duration.ofSeconds(10))) {
                Lock competingLock = separateLease.getLock(resourceId);
                threadStartedWaiting.countDown();
                // This call will block until interrupted
                assertThrows(
                    InterruptedException.class,
                    competingLock::lockInterruptibly,
                    "Should throw InterruptedException");
              }
            });

    // Ensure the thread has started and is waiting for the lock
    assertTrue(
        threadStartedWaiting.await(2, TimeUnit.SECONDS),
        "Competing thread did not start waiting in time");

    // Interrupt the waiting thread
    executor.shutdownNow(); // Sends an interrupt signal

    assertTrue(
        executor.awaitTermination(2, TimeUnit.SECONDS), "Executor did not terminate correctly");

    // Verify that the future completed without a different exception
    try {
      future.get();
    } catch (ExecutionException e) {
      if (!(e.getCause() instanceof InterruptedException)) {
        fail("Expected InterruptedException, but got " + e.getCause());
      }
      // This is expected because lockInterruptibly throws it.
    } catch (CancellationException e) {
      // This is also a possible outcome of shutdownNow, which is acceptable.
    }

    lock.unlock();
    lock.close();
  }

  @Test
  void testReentrantLock_acquiresLockMultipleTimes() throws Exception {
    Lock lock = lease.getLock(resourceId);
    // First lock
    lock.lock();
    try {
      // Reentrant lock
      assertDoesNotThrow(
          () -> {
            lock.lock();
            // Both locks are held by the same lease/thread context
            lock.unlock(); // Release inner lock
          },
          "Reentrant lock acquisition should not throw an exception or block");
    } finally {
      lock.unlock(); // Release outer lock
    }
    assertFalse(lock.isHeldByCurrentLease(), "Lock should be fully released");
    lock.close();
  }

  @Test
  @Timeout(10)
  void testLock_canBeAcquiredAfterLeaseExpires() throws Exception {
    String shortLivedResourceId = "short-lived-lock-" + UUID.randomUUID();
    Lock lock1;

    // Use a very short-lived lease to simulate a client dying
    try (Lease shortLease = atomaClient.grantLease(Duration.ofSeconds(2))) {
      lock1 = shortLease.getLock(shortLivedResourceId);
      lock1.lock();
      assertTrue(lock1.isHeldByCurrentLease(), "Lock should be held by the short-lived lease");
      // Do not unlock, let the lease expire
    }

    // Wait for the lease to expire. Adding a small buffer.
    Thread.sleep(2500);

    // Now, a new client with a new lease should be able to acquire the lock
    Lock lock2 = lease.getLock(shortLivedResourceId);
    assertDoesNotThrow(
        () -> {
          assertTrue(
              lock2.tryLock(2, TimeUnit.SECONDS),
              "Should be able to acquire the lock after the previous lease expired");
          try {
            assertTrue(lock2.isHeldByCurrentLease());
          } finally {
            lock2.unlock();
          }
        },
        "Acquiring lock after expiry should not throw exception");
    lock2.close();
  }
}
