package atoma.api;

import atoma.api.lock.Lock;
import atoma.api.lock.ReadWriteLock;
import atoma.api.synchronizer.Semaphore;

import java.time.Duration;

/**
 * Represents a session lease between a client and the Atoma coordination service.
 *
 * <p>A {@code Lease} is fundamental for the secure and reliable operation of all distributed primitives.
 * It maintains its validity through an automatic renewal mechanism. In the event of a client failure,
 * the lease mechanism ensures that any resources held by the client (such as locks) are eventually
 * and automatically reclaimed by the system, thereby preventing deadlocks and resource leaks.
 *
 * <p>This abstract class implements {@link AutoCloseable}, making it suitable for use in
 * try-with-resources statements. This ensures that the lease is cleanly and automatically
 * revoked when the client's scope exits, promoting proper resource management.
 */
public abstract class Lease extends Resourceful {

  /**
   * Retrieves or creates a distributed mutex lock instance associated with this lease.
   * The lock is identified by a unique {@code resourceId}.
   *
   * @param resourceId The unique identifier for the lock resource.
   * @return A distributed mutex {@link Lock} instance.
   */
  public abstract Lock getLock(String resourceId);

  /**
   * Retrieves or creates a distributed read-write lock instance associated with this lease.
   * The read-write lock is identified by a unique {@code resourceId}.
   *
   * @param resourceId The unique identifier for the read-write lock resource.
   * @return A distributed {@link ReadWriteLock} instance.
   */
  public abstract ReadWriteLock getReadWriteLock(String resourceId);

  /**
   * Retrieves or creates a distributed semaphore instance associated with this lease.
   * The semaphore is identified by a unique {@code resourceId} and initialized with
   * a specified number of permits.
   *
   * @param resourceId The unique identifier for the semaphore resource.
   * @param initialPermits The initial number of permits available for the semaphore.
   * @return A distributed {@link Semaphore} instance.
   */
  public abstract Semaphore getSemaphore(String resourceId, int initialPermits);

  /**
   * Retrieves the globally unique identifier for this lease.
   * This ID is used by the coordination service to identify the ownership of distributed resources.
   *
   * @return The unique ID of this lease.
   */
  public abstract String getResourceId();

  /**
   * Explicitly and permanently revokes this lease.
   *
   * <p>Calling this method immediately notifies the coordination service that this lease
   * has terminated. This action triggers the cleanup of all distributed resources
   * (e.g., owned locks, acquired permits) associated with this lease.
   * This operation is idempotent.
   */
  public abstract void revoke();

  /**
   * Executes time-to-live (TTL) operations for this lease.
   * This method is typically called internally by the lease management mechanism
   * to periodically update or refresh the lease's status with the coordination service.
   */
  public abstract void timeToLive();

  /**
   * Returns the configured time-to-live duration for this lease.
   * This duration specifies how long the lease remains valid without renewal.
   *
   * @return The {@link Duration} of the lease's time-to-live.
   */
  public abstract Duration getTtlDuration();

  /**
   * Checks if this lease has been explicitly revoked by the client.
   *
   * <p>Note: A return value of {@code false} does not guarantee that the lease is still valid
   * on the server side. The lease might have expired due to network partitions, server issues,
   * or other reasons beyond the client's explicit control. This method primarily reflects
   * whether the client has actively invoked {@link #revoke()}.
   *
   * @return {@code true} if {@link #revoke()} has been called for this lease; {@code false} otherwise.
   */
  public abstract boolean isRevoked();

  /**
   * Implements the {@link AutoCloseable} interface.
   *
   * <p>When this lease is used in a try-with-resources statement, this method is automatically
   * invoked upon exiting the try block. Its behavior is equivalent to calling {@link #revoke()},
   * ensuring that the lease is properly terminated and associated resources are released.
   *
   * @throws Exception if an error occurs during the revocation process.
   */
  @Override
  public void close() throws Exception {
    revoke();
  }
}
