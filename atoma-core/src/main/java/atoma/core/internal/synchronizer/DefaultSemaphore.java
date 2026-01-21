package atoma.core.internal.synchronizer;

import atoma.api.AtomaException;
import atoma.api.OperationTimeoutException;
import atoma.api.coordination.CoordinationStore;
import atoma.api.coordination.Resource;
import atoma.api.coordination.ResourceChangeEvent;
import atoma.api.coordination.Subscription;
import atoma.api.coordination.command.SemaphoreCommand;
import atoma.api.synchronizer.Semaphore;
import atoma.core.internal.ThreadUtils;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The default client-side implementation of a distributed {@link Semaphore}.
 *
 * <p>This class provides the full functionality of a traditional semaphore but for a distributed
 * environment, coordinated via a backend {@link CoordinationStore}. It correctly handles blocking
 * waits, timeouts, and interruptibility.
 *
 * <p><b>Resource Management:</b> This class implements {@link AutoCloseable}. It is crucial to
 * close the semaphore instance when it is no longer needed (e.g., using a try-with-resources block)
 * to release the underlying network subscription and prevent resource leaks.
 *
 * <h3>Implementation Details</h3>
 *
 * <h4>Design Philosophy: "Stateless Wait"</h4>
 *
 * <p>A critical design choice for a distributed semaphore client is whether to maintain a local
 * cache of the server's {@code available_permits} count. This implementation deliberately chooses a
 * <strong>"stateless wait"</strong> approach for correctness and robustness, where the backend
 * service is the single source of truth.
 *
 * <p>An alternative "stateful" approach, where the client caches the permit count locally to avoid
 * network calls, suffers from inherent race conditions in a distributed environment. A client might
 * decide to proceed based on its stale local cache, only to have the authoritative network call
 * fail because another client acted first. This leads to complex and error-prone
 * state-synchronization and rollback logic.
 *
 * <p>Instead, this implementation adheres to the following robust pattern:
 *
 * <ol>
 *   <li><b>Single Source of Truth:</b> The backend coordination service (e.g., MongoDB) is treated
 *       as the single, authoritative source of truth for the permit count. The client holds no
 *       authoritative local state.
 *   <li><b>Optimistic Execution:</b> The {@code acquire} method always optimistically attempts to
 *       acquire permits from the server first.
 *   <li><b>"Smart" Listener:</b> If the optimistic attempt fails, the thread waits. It is only
 *       woken up by a "smart" listener that inspects {@code UPDATE} events and only signals waiters
 *       if the {@code available_permits} count has actually <strong>increased</strong>. This
 *       prevents inefficient wake-ups when other clients acquire permits.
 *   <li><b>Looping as the Guarantee:</b> Upon waking, a thread makes no assumptions. It simply
 *       loops back to the optimistic execution step to re-query the authoritative source. This loop
 *       is the ultimate guarantee of correctness against any race conditions or spurious wake-ups.
 * </ol>
 *
 * <h4>Signaling Strategy: {@code signalAll()} vs. {@code signal()}</h4>
 *
 * <p>Unlike a mutex (which has a single permit), a semaphore can have many. A single {@code
 * release} operation can increase the number of available permits sufficiently to satisfy
 * <strong>multiple</strong> waiting threads with varying permit requests.
 *
 * <p>If {@code signal()} were used, only one thread (the longest-waiting) would be woken up. Even
 * if it acquires its permits, other threads that could also have been satisfied by the remaining
 * permits would be left waiting unnecessarily. This would lead to under-utilization of the
 * semaphore's resources and reduced throughput.
 *
 * <p>Therefore, this implementation uses {@code signalAll()} to wake up all waiting threads. While
 * this creates a "thundering herd" where all threads re-attempt acquisition, it is the correct
 * approach for a semaphore. It guarantees that all newly available permits are contended for,
 * maximizing resource utilization and ensuring liveness for all waiting threads.
 *
 * @see atoma.api.synchronizer.Semaphore
 * @see atoma.api.coordination.CoordinationStore
 */
public class DefaultSemaphore extends Semaphore {

  private final String resourceId;
  private final String leaseId;
  private final int initialPermits;
  private final CoordinationStore coordination;
  private final Subscription subscription;

  private final ReentrantLock localLock = new ReentrantLock(true);
  private final Condition permitsAvailable = localLock.newCondition();

  public DefaultSemaphore(
      String resourceId, String leaseId, int initialPermits, CoordinationStore coordination) {
    this.resourceId = resourceId;
    this.leaseId = leaseId;
    this.initialPermits = initialPermits;
    this.coordination = coordination;

    this.subscription =
        coordination.subscribe(
            Semaphore.class, // TODO: This should be a defined constant for Semaphore
            resourceId,
            event -> {
              boolean shouldSignal = false;
              if (event.getType() == ResourceChangeEvent.EventType.DELETED) {
                shouldSignal = true;
              } else if (event.getType() == ResourceChangeEvent.EventType.UPDATED) {
                Optional<Resource> oldNodeOpt = event.getOldNode();
                Optional<Resource> newNodeOpt = event.getNewNode();

                if (oldNodeOpt.isPresent() && newNodeOpt.isPresent()) {
                  Integer oldPermits = oldNodeOpt.get().get("available_permits");
                  Integer newPermits = newNodeOpt.get().get("available_permits");

                  // Only signal if the number of available permits has actually increased.
                  if (oldPermits != null && newPermits != null && newPermits > oldPermits) {
                    shouldSignal = true;
                  }
                }
              }

              if (shouldSignal) {
                localLock.lock();
                try {
                  permitsAvailable.signalAll();
                } finally {
                  localLock.unlock();
                }
              }
            });
  }

  /**
   * Acquires the given number of permits from this semaphore, blocking indefinitely until all are
   * available, or the thread is {@linkplain Thread#interrupt interrupted}.
   *
   * @param permits the number of permits to acquire (must be non-negative)
   * @throws InterruptedException if the current thread is interrupted while waiting
   * @throws IllegalArgumentException if {@code permits} is negative
   */
  @Override
  public void acquire(int permits) throws InterruptedException {
    try {
      doAcquire(permits, -1, null);
    } catch (TimeoutException e) {
      // This should not happen in a non-timed acquire.
      throw new AssertionError("Timeout occurred in non-timed acquire method", e);
    }
  }

  /**
   * Acquires the given number of permits from this semaphore, waiting up to the specified wait time
   * if necessary for the permits to become available.
   *
   * @param permits the number of permits to acquire (must be non-negative)
   * @param waitTime the maximum time to wait for the permits. If non-positive, the method will not
   *     wait.
   * @param timeUnit the time unit of the {@code waitTime} argument
   * @throws InterruptedException if the current thread is interrupted while waiting
   * @throws IllegalArgumentException if {@code permits} is negative
   * @throws RuntimeException if the wait timed out, wrapping a {@link TimeoutException}
   */
  @Override
  public void acquire(int permits, Long waitTime, TimeUnit timeUnit) throws InterruptedException {
    try {
      doAcquire(permits, waitTime, timeUnit);
    } catch (TimeoutException e) {
      // For API consistency, we let InterruptedException propagate but wrap TimeoutException.
      throw new RuntimeException("Acquisition timed out", e);
    }
  }

  /**
   * Releases the given number of permits, returning them to the semaphore. This is a non-blocking
   * operation.
   *
   * @param permits the number of permits to release (must be non-negative)
   * @throws IllegalArgumentException if {@code permits} is negative
   * @throws RuntimeException if the release command fails due to a server-side error, wrapping an
   *     {@link AtomaException} or {@link IllegalStateException}.
   */
  @Override
  public void release(int permits) {
    if (permits < 0) throw new IllegalArgumentException("permits must be non-negative");
    if (permits == 0) return;

    String holderId = ThreadUtils.getCurrentThreadId();
    var releaseCommand = new SemaphoreCommand.Release(permits, holderId, leaseId);
    try {
      coordination.execute(resourceId, releaseCommand);
    } catch (AtomaException e) {
      // Release failures are critical and indicate a state problem.
      throw new RuntimeException("Failed to release permits due to a coordination error", e);
    }
  }

  /**
   * Returns the initial number of permits this semaphore was configured with.
   *
   * <p>Note: This does not reflect the current number of available permits.
   *
   * @return the initial number of permits
   */
  @Override
  public int getPermits() {
    return initialPermits;
  }

  @Override
  public String getResourceId() {
    return resourceId;
  }

  @Override
  public String getLeaseId() {
    return leaseId;
  }

  /**
   * Closes this semaphore and releases the underlying subscription resource. This should be called
   * when the semaphore is no longer needed to prevent resource leaks.
   */
  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      if (this.subscription != null) {
        this.subscription.close();
      }
    }
  }

  private void doAcquire(int permits, long time, TimeUnit unit)
      throws InterruptedException, TimeoutException {
    if (permits < 0) throw new IllegalArgumentException("permits must be non-negative");
    if (permits == 0) return;

    final boolean timed = (unit != null && time >= 0);
    long remainingNanos = timed ? unit.toNanos(time) : 0;

    String holderId = ThreadUtils.getCurrentThreadId();
    var acquireCommand =
        new SemaphoreCommand.Acquire(permits, holderId, leaseId, time, unit, initialPermits);

    for (; ; ) {
      try {
        SemaphoreCommand.AcquireResult result = coordination.execute(resourceId, acquireCommand);
        if (result.acquired()) {
          return; // Success
        }
      } catch (AtomaException e) {
        Throwable cause = e;
        while (cause != null) {
          if (cause instanceof OperationTimeoutException) {
            throw new TimeoutException(
                "Semaphore acquire command timed out during server-side execution.");
          }
          cause = cause.getCause();
        }
        throw new RuntimeException(
            "Failed to execute acquire command due to a coordination error", e);
      }

      // If acquisition failed, prepare to wait.
      if (timed && remainingNanos <= 0) {
        throw new TimeoutException("Unable to acquire permits within the specified time.");
      }

      localLock.lock();
      try {
        if (timed) {
          if (remainingNanos <= 0) throw new TimeoutException("Wait time elapsed.");
          long start = System.nanoTime();
          if (!permitsAvailable.await(remainingNanos, TimeUnit.NANOSECONDS)) {
            throw new TimeoutException("Wait time elapsed before signal.");
          }
          remainingNanos -= (System.nanoTime() - start);
        } else {
          permitsAvailable.await();
        }
      } finally {
        localLock.unlock();
      }
    }
  }
}
