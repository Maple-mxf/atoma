package atoma.core.internal.lock;

import atoma.api.AtomaException;
import atoma.api.OperationTimeoutException;
import atoma.api.coordination.CoordinationStore;
import atoma.api.coordination.Resource;
import atoma.api.coordination.ResourceChangeEvent;
import atoma.api.coordination.Subscription;
import atoma.api.coordination.command.LockCommand;
import atoma.api.lock.Lock;
import atoma.core.internal.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This implementation assumes the parent `Lock` interface can be modified to extend `AutoCloseable`
 * or `Closeable` for proper resource management.
 *
 * <p>Note: Mutex lock and thread are related. In other words, The mutex-lock acquired by 'A'
 * thread. Only thread A can invoked {@link DefaultMutexLock#unlock()} method successful.
 */
public class DefaultMutexLock extends Lock {

  private final Logger log = LoggerFactory.getLogger(DefaultMutexLock.class);

  private final String resourceId;
  private final String leaseId;
  private final CoordinationStore coordination;
  private final Subscription subscription;

  // A reentrant count-times for the thread.
  private final ThreadLocal<Integer> reentrancyCounter = ThreadLocal.withInitial(() -> 0);

  // A fair lock to coordinate local threads waiting for the distributed lock.
  private final ReentrantLock localLock = new ReentrantLock(true);
  private final Condition remoteLockAvailable = localLock.newCondition();

  // State variable representing the client's view of the remote lock's status.
  // It is protected by the localLock.
  private boolean isRemoteLockHeld = false;

  // The logical-lock-version represent lock-data's latest version.
  private volatile long clientLogicalLockVersion = 0L;

  /**
   * @param resourceId Mutex-lock resource-id
   * @param leaseId The lease associated with current thread.
   * @param coordination The instance for storing and coordinating state data
   */
  public DefaultMutexLock(String resourceId, String leaseId, CoordinationStore coordination) {
    this.resourceId = resourceId;
    this.leaseId = leaseId;
    this.coordination = coordination;
    this.subscription =
        coordination.subscribe(
            // API design issue: this parameter is unclear
            Lock.class,
            resourceId,
            event -> {
              if (log.isDebugEnabled()) {
                log.debug(
                    "Mutex lock received an event. Event-type: {}. Latest version: {} ",
                    event.getType(),
                    event.getNewNode().map(Resource::getVersion).orElse(-1L));
              }

              if (Objects.requireNonNull(event.getType())
                  == ResourceChangeEvent.EventType.DELETED) {
                localLock.lock();
                try {
                  // The remote lock is now free. Update our local state view.
                  isRemoteLockHeld = false;

                  // Reset the latest value because of delete operation.
                  advancingLatestVersion(0L);

                  // Wake up one waiting thread to re-compete for the lock.
                  remoteLockAvailable.signal();
                } finally {
                  localLock.unlock();
                }
              } else {
                event
                    .getNewNode()
                    .ifPresent(
                        n -> {
                          long version = n.getVersion();
                          advancingLatestVersion(version);
                        });
              }
            });
  }

  @Override
  public String getLeaseId() {
    return leaseId;
  }

  @Override
  public String getResourceId() {
    return resourceId;
  }

  @Override
  public void lock() {
    // This method implements non-interruptible lock acquisition, matching
    // java.util.concurrent.locks.Lock.
    boolean interrupted = false;
    try {
      for (; ; ) {
        try {
          acquire(-1, null);
          break; // Lock acquired
        } catch (InterruptedException e) {
          interrupted = true;
        } catch (TimeoutException e) {
          throw new AssertionError("Timeout occurred in non-timed lock method", e);
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void lock(long time, TimeUnit unit) throws InterruptedException, TimeoutException {
    Objects.requireNonNull(unit, "TimeUnit cannot be null for a timed lock");
    acquire(time, unit);
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    try {
      acquire(-1, null);
    } catch (TimeoutException e) {
      throw new AssertionError("Timeout occurred in non-timed lock method", e);
    }
  }

  /**
   * Advancing the latest version
   *
   * @param latestVersion The latest version returned by storage.
   */
  private void advancingLatestVersion(long latestVersion) {
    if (latestVersion > this.clientLogicalLockVersion || latestVersion == 0L)
      clientLogicalLockVersion = latestVersion;
  }

  /**
   * Private helper method containing the core logic for acquiring the distributed lock.
   *
   * <p>The acquisition strategy is as follows:
   *
   * <ol>
   *   <li><b>Reentrancy Check:</b> First, it checks a ThreadLocal counter. If the current thread
   *       already holds the lock, the counter is incremented and the method returns immediately.
   *   <li><b>Optimistic Attempt:</b> The method begins with an optimistic, non-blocking network
   *       call to acquire the lock. This ensures fast acquisition in the common, uncontended case.
   *   <li><b>Coordinated Wait:</b> If the optimistic attempt fails, the thread prepares to wait. It
   *       acquires a local, fair ReentrantLock to coordinate with other local threads also waiting
   *       for the same distributed lock. It sets a shared flag, {@code isRemoteLockHeld}, to true
   *       and waits on a Condition in a loop.
   *   <li><b>Wake-up and Contention Management:</b> When the distributed lock is released, a
   *       listener calls {@code signal()} on the Condition. By using {@code signal()} instead of
   *       {@code signalAll()}, only one waiting thread (the one that has waited the longest, due to
   *       the fair lock) is woken up. This single thread then loops back to make another
   *       acquisition attempt. This approach elegantly avoids the "thundering herd" problem,
   *       preventing multiple threads from competing unnecessarily for the lock after a single
   *       release event.
   * </ol>
   *
   * @param time the maximum time to wait for the lock
   * @param unit the time unit of the {@code time} argument. If null, wait indefinitely.
   * @throws InterruptedException if the current thread is interrupted
   * @throws TimeoutException if the lock could not be acquired within the specified time
   */
  private void acquire(long time, TimeUnit unit) throws InterruptedException, TimeoutException {
    if (reentrancyCounter.get() > 0) {
      reentrancyCounter.set(reentrancyCounter.get() + 1);
      return;
    }

    final boolean timed = (unit != null);
    long remainingNanos = timed ? unit.toNanos(time) : 0;

    String holderId = ThreadUtils.getCurrentThreadId();
    var acquireCommand = new LockCommand.Acquire(holderId, leaseId, -1, TimeUnit.MILLISECONDS);

    LockCommand.AcquireResult result;
    Retry:
    for (; ; ) {
      try {
        result = coordination.execute(resourceId, acquireCommand);

        // Acquisition success.
        if (result.acquired()) {
          reentrancyCounter.set(1);
          return;
        }

        // Acquisition failed because an unexpected error.
        // Which will be retrying if latest-version is negatived.
        if (result.serverLogicalLatestVersion() < 0 || clientLogicalLockVersion == 0L) {
          continue Retry;
        }
      } catch (AtomaException e) {
        // Check if the exception or its cause is a server-side operation timeout.
        Throwable cause = e;
        while (cause != null) {
          if (cause instanceof OperationTimeoutException) {
            // Translate the low-level exception to the one declared in our public API contract.
            throw new TimeoutException(
                "Lock acquisition command timed out during server-side execution.");
          }
          cause = cause.getCause();
        }
        // If it's another type of coordination error, wrap it as a runtime failure.
        throw new RuntimeException("Failed to execute lock command due to a coordination error", e);
      }

      if (timed && remainingNanos <= 0) {
        throw new TimeoutException("Unable to acquire lock within the specified time.");
      }

      localLock.lock();
      try {
        isRemoteLockHeld = true;
        while (isRemoteLockHeld) {
          if (timed) {
            if (remainingNanos <= 0) {
              throw new TimeoutException("Unable to acquire lock within the specified time.");
            }
            long start = System.nanoTime();
            if (!remoteLockAvailable.await(remainingNanos, TimeUnit.NANOSECONDS)) {
              throw new TimeoutException("Unable to acquire lock within the specified time.");
            }
            remainingNanos -= (System.nanoTime() - start);
          } else {
            if (result.serverLogicalLatestVersion() >= this.clientLogicalLockVersion)
              remoteLockAvailable.await();
            else continue Retry;
          }
        }
      } finally {
        localLock.unlock();
      }
    }
  }

  @Override
  public void unlock() {
    Integer count = reentrancyCounter.get();
    if (count <= 0) {
      throw new IllegalMonitorStateException(
          "Current thread does not hold the lock: " + resourceId);
    }

    count--;

    if (count > 0) {
      reentrancyCounter.set(count);
      return;
    }

    reentrancyCounter.remove();
    String holderId = ThreadUtils.getCurrentThreadId();
    var releaseCommand = new LockCommand.Release(holderId);
    coordination.execute(resourceId, releaseCommand);
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      if (this.subscription != null) {
        this.subscription.close();
      }
    }
  }
}
