package atoma.api.synchronizer;

import atoma.api.Leasable;
import atoma.api.OperationTimeoutException;

import java.util.concurrent.TimeUnit;

/**
 * A distributed counting semaphore.
 *
 * <p>Conceptually, a semaphore maintains a set of permits. The {@link #acquire(int)} method
 * blocks if necessary until enough permits are available, and then takes them. The {@link
 * #release(int)} method adds permits, potentially releasing a blocking acquirer.
 *
 * <p>This is an abstract class for a distributed implementation, meaning the state of the semaphore
 * is managed consistently across multiple clients or services.
 *
 * @see java.util.concurrent.Semaphore
 */
public abstract class Semaphore extends Leasable {

  /**
   * Acquires the given number of permits from this semaphore, blocking until all are available.
   *
   * @param permits the number of permits to acquire (must be positive)
   * @throws InterruptedException if the current thread is interrupted while waiting
   * @throws IllegalArgumentException if {@code permits} is not a positive number
   */
  public abstract void acquire(int permits) throws InterruptedException;

  /**
   * Acquires the given number of permits from this semaphore, blocking until all are available or
   * the specified waiting time elapses.
   *
   * @param permits the number of permits to acquire (must be positive)
   * @param waitTime the maximum time to wait for the permits
   * @param timeUnit the time unit of the {@code waitTime} argument
   * @throws InterruptedException if the current thread is interrupted while waiting
   * @throws OperationTimeoutException if the waiting time elapses before the permits can be acquired
   * @throws IllegalArgumentException if {@code permits} is not a positive number
   */
  public abstract void acquire(int permits, Long waitTime, TimeUnit timeUnit)
      throws InterruptedException;

  /**
   * Releases the given number of permits, returning them to the semaphore.
   *
   * <p>This may result in a waiting thread being unblocked.
   *
   * @param permits the number of permits to release (must be positive)
   * @throws IllegalArgumentException if {@code permits} is not a positive number
   */
  public abstract void release(int permits);

  /**
   * Returns the initial number of permits this semaphore was created with.
   *
   * @return the initial number of permits
   */
  public abstract int getPermits();

  /**
   * Acquires and returns all permits that are immediately available in a non-blocking manner.
   *
   * @return the number of permits drained
   */
  public abstract int drainPermits();

  /**
   * Returns the current number of permits available in this semaphore.
   *
   * <p>This value is typically used for monitoring and debugging purposes. In a distributed system,
   * this may be an estimate, as the value could change immediately after being read.
   *
   * @return the number of permits available
   */
  public abstract int availablePermits();
}
