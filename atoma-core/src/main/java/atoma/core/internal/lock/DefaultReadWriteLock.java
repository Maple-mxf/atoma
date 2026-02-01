package atoma.core.internal.lock;

import atoma.api.AtomaException;
import atoma.api.OperationTimeoutException;
import atoma.api.coordination.CoordinationStore;
import atoma.api.coordination.ResourceChangeEvent;
import atoma.api.coordination.Subscription;
import atoma.api.coordination.command.Command;
import atoma.api.coordination.command.LockCommand;
import atoma.api.coordination.command.ReadWriteLockCommand;
import atoma.api.lock.Lock;
import atoma.api.lock.ReadWriteLock;
import atoma.core.internal.ThreadUtils;
import com.google.common.annotations.Beta;
import com.google.errorprone.annotations.MustBeClosed;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The default client-side implementation of the {@link atoma.api.lock.ReadWriteLock} interface.
 *
 * <h3>Architectural Overview</h3>
 *
 * This class acts as a factory and container for the concrete {@link Lock} implementations. It
 * manages the state shared between both lock types, including the connection to the {@link
 * CoordinationStore} and the local concurrency primitives used for thread coordination.
 *
 * <h4>Shared Local State &amp; Dual Conditions</h4>
 *
 * To ensure proper interaction between waiting readers and writers, this class uses a single,
 * shared, fair {@link ReentrantLock} to protect local state. It maintains two distinct {@link
 * Condition} objects:
 *
 * <ul>
 *   <li><b>readerCondition:</b> Threads waiting to acquire a read lock will wait on this condition.
 *   <li><b>writerCondition:</b> Threads waiting to acquire a write lock will wait on this
 *       condition.
 * </ul>
 *
 * <h4>Internal Implementation</h4>
 *
 * Both the {@code ReadLock} and {@code WriteLock} are implemented as inner classes extending a
 * common {@code AbstractLock} base class. This abstract class encapsulates the complex logic for
 * re-entrancy, optimistic acquisition attempts, exception handling, and waiting on conditions, thus
 * maximizing code reuse. The concrete implementations only need to provide the specific {@link
 * atoma.api.coordination.command.Command} to be executed for their respective operations.
 *
 * @see atoma.core.internal.lock.DefaultReadWriteLock.AbstractLock
 * @see atoma.api.lock.ReadWriteLock
 */
@Beta
@ThreadSafe
public class DefaultReadWriteLock extends ReadWriteLock {

  // --- Shared State for both Read and Write Locks ---
  private final String resourceId;
  private final String leaseId;
  private final CoordinationStore coordination;
  private final Subscription subscription;
  private final ReentrantLock localLock = new ReentrantLock(true);

  // Separate conditions for readers and writers for fine-grained, high-performance signaling.
  private final Condition readerCondition = localLock.newCondition();
  private final Condition writerCondition = localLock.newCondition();

  private final ReadLockImpl readLock;
  private final WriteLockImpl writeLock;

  // The logical-lock-version represents lock-data's latest version.
  @GuardedBy("localLock")
  private volatile long clientLogicalLockVersion = 0L;

  // The value of state that represent read-write lock's state.
  // The value is 0 if the read-write lock can try read-lock or write lock.
  // The value is 1 if the lock acquired by read thread.
  // The value is 2 if the lock acquired by write thread.
  private volatile int state;
  private static final int STATE_AVAILABLE_RW = 0;
  private static final int STATE_AVAILABLE_R = 1;
  private static final int STATE_UNAVAILABLE_RW = 2;

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
   * Constructs a new DefaultReadWriteLock for a given resource.
   *
   * <p>This constructor initializes the shared state for the lock and establishes a single,
   * long-lived subscription to the resource's change stream in the {@link CoordinationStore}.
   *
   * <h4>Subscription and Signaling Logic</h4>
   *
   * The subscription listener is the core of the client-side waiting mechanism. It implements a
   * sophisticated, writer-preference signaling strategy to ensure both correctness and high
   * performance under contention:
   *
   * <ol>
   *   <li>The listener reacts to {@code UPDATE} and {@code DELETE} events on the lock document.
   *   <li>It inspects the event payload to determine if the lock state has changed in a way that
   *       would permit new acquisitions (e.g., a write lock was released, or the last read lock was
   *       released).
   *   <li><b>Writer-Preference Policy:</b> Upon such a change, it checks if any threads are waiting
   *       on the {@code writerCondition}.
   *       <ul>
   *         <li>If writers are waiting, it calls {@code signal()} on the {@code writerCondition},
   *             waking up a single writer to give it priority and prevent starvation.
   *         <li>If no writers are waiting, it calls {@code signalAll()} on the {@code
   *             readerCondition}, waking up all waiting readers to allow for maximum concurrency.
   *       </ul>
   * </ol>
   *
   * This strategy avoids the "thundering herd" problem while correctly enabling shared access for
   * readers when appropriate.
   *
   * @param resourceId The unique ID of the resource to lock.
   * @param leaseId The lease ID of the client session.
   * @param coordination The coordination store used to execute commands and listen for events.
   */
  @MustBeClosed
  public DefaultReadWriteLock(String resourceId, String leaseId, CoordinationStore coordination) {
    this.resourceId = resourceId;
    this.leaseId = leaseId;
    this.coordination = coordination;

    this.readLock = new ReadLockImpl(this);
    this.writeLock = new WriteLockImpl(this);

    this.state = STATE_AVAILABLE_RW;

    this.subscription =
        coordination.subscribe(
            ReadWriteLock.class,
            resourceId,
            event -> {
              if (event.getType() == ResourceChangeEvent.EventType.DELETED) {
                advancingLatestVersion(0L);
              } else {
                event.getNewNode().ifPresent(n -> advancingLatestVersion(n.getVersion()));
              }

              if (event.getType() == ResourceChangeEvent.EventType.UPDATED
                  || event.getType() == ResourceChangeEvent.EventType.DELETED) {

                boolean isNowFullyFree =
                    event
                        .getNewNode()
                        .map(
                            node -> {
                              Map<String, Object> data = node.getData();
                              boolean writeLockExists = data.get("write_lock") != null;
                              Object readLocksObj = data.get("read_locks");
                              boolean readLocksExist =
                                  readLocksObj instanceof List
                                      && !((List<?>) readLocksObj).isEmpty();
                              return !writeLockExists && !readLocksExist;
                            })
                        .orElse(true);

                if (isNowFullyFree) {
                  localLock.lock();
                  state = STATE_AVAILABLE_RW;
                  try {
                    if (localLock.hasWaiters(writerCondition)) {
                      writerCondition.signal();
                    } else {
                      readerCondition.signalAll();
                    }
                  } finally {
                    localLock.unlock();
                  }
                } else {
                  boolean isWriteLocked =
                      event
                          .getNewNode()
                          .map(n -> n.getData().get("write_lock") != null)
                          .orElse(false);

                  localLock.lock();
                  try {
                    if (!isWriteLocked) {
                      state = STATE_AVAILABLE_R;
                      readerCondition.signalAll();
                    } else {
                      state = STATE_UNAVAILABLE_RW;
                    }
                  } finally {
                    localLock.unlock();
                  }
                }
              }
            });
  }

  @Override
  public Lock readLock() {
    return readLock;
  }

  @Override
  public Lock writeLock() {
    return writeLock;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      if (this.subscription != null) {
        this.subscription.close();
        this.readLock.close();
        this.writeLock.close();
      }
    }
  }

  @Override
  public String getLeaseId() {
    return leaseId;
  }

  @Override
  public String getResourceId() {
    return resourceId;
  }

  // --- Abstract Base Class for Lock Implementations ---

  private abstract static class AbstractLock extends Lock {
    protected final DefaultReadWriteLock parent;
    private final ThreadLocal<Integer> reentrancyCounter = ThreadLocal.withInitial(() -> 0);

    protected AbstractLock(DefaultReadWriteLock parent) {
      this.parent = parent;
    }

    protected abstract Command<LockCommand.AcquireResult> buildAcquireCommand(
        String holderId, long timeout, TimeUnit timeUnit);

    protected abstract Command<Void> buildReleaseCommand(String holderId);

    protected abstract Condition getCondition();

    protected abstract boolean lockAvailable();

    @Override
    public String getLeaseId() {
      return parent.leaseId;
    }

    @Override
    public String getResourceId() {
      return parent.resourceId;
    }

    @Override
    public void lock() {
      boolean interrupted = false;
      try {
        for (; ; ) {
          try {
            acquire(-1, null);
            break;
          } catch (InterruptedException e) {
            interrupted = true;
          } catch (TimeoutException e) {
            throw new AssertionError("Timeout in non-timed method", e);
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
      if (time == 0) throw new TimeoutException();
      Objects.requireNonNull(unit);
      acquire(time, unit);
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      try {
        acquire(-1, null);
      } catch (TimeoutException e) {
        throw new AssertionError("Timeout in non-timed method", e);
      }
    }

    @Override
    public void unlock() {
      Integer count = reentrancyCounter.get();
      if (count <= 0) {
        throw new IllegalMonitorStateException(
            "Current thread does not hold the lock: " + parent.resourceId);
      }
      count--;

      if (count > 0) {
        reentrancyCounter.set(count);
        return;
      }

      reentrancyCounter.remove();
      String holderId = ThreadUtils.getCurrentThreadId();
      var releaseCommand = buildReleaseCommand(holderId);
      parent.coordination.execute(parent.resourceId, releaseCommand);
    }

    @Override
    public void close() {
      parent.close();
    }

    /**
     * Private helper method containing the core logic for acquiring the distributed lock. This
     * method orchestrates re-entrancy checks, optimistic acquisition attempts, and the coordinated
     * local waiting mechanism.
     *
     * <p>The acquisition strategy is as follows:
     *
     * <ol>
     *   <li><b>Reentrancy Check:</b> First, it checks a ThreadLocal counter. If the current thread
     *       already holds this specific lock (read or write), the counter is incremented and the
     *       method returns immediately.
     *   <li><b>Optimistic Attempt:</b> The method enters an infinite loop and begins with an
     *       optimistic, non-blocking network call to acquire the lock. This ensures fast
     *       acquisition in the common, uncontended case. If successful, the lock is granted.
     *   <li><b>Exception Translation:</b> It wraps the network call in a try-catch block to
     *       translate any storage-layer {@link atoma.api.OperationTimeoutException} into the
     *       standard, checked {@link java.util.concurrent.TimeoutException} required by the Lock
     *       API contract.
     *   <li><b>Coordinated Wait:</b> If the optimistic attempt fails, the thread prepares to wait.
     *       It acquires a local, fair ReentrantLock (shared between read and write locks) and waits
     *       on the specific {@link Condition} object provided by the concrete implementation
     *       ({@code getCondition()}). The loop structure ensures that upon waking up, the thread
     *       will loop back to the optimistic attempt.
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

      if (time == 0L)
        throw new TimeoutException("Lock command timed out during server-side execution.");

      final boolean timed = (unit != null && time > 0L);
      long start = System.nanoTime(), clockTimeout = timed ? unit.toNanos(time) : -1L;

      String holderId = ThreadUtils.getCurrentThreadId();

      final Condition condition = getCondition();
      LockCommand.AcquireResult result;
      Retry:
      for (; ; ) {
        long remainingNanos = timed ? (clockTimeout - (System.nanoTime() - start)) : -1L;
        try {
          var acquireCommand = buildAcquireCommand(holderId, remainingNanos, TimeUnit.NANOSECONDS);
          result = parent.coordination.execute(parent.resourceId, acquireCommand);
          if (result.acquired()) {
            reentrancyCounter.set(1);
            return;
          }

          remainingNanos = timed ? (clockTimeout - (System.nanoTime() - start)) : -1L;

        } catch (AtomaException e) {
          Throwable cause = e;
          while (cause != null) {
            if (cause instanceof OperationTimeoutException) {
              throw new TimeoutException("Lock command timed out during server-side execution.");
            }
            cause = cause.getCause();
          }
          throw new RuntimeException("Failed to execute lock command", e);
        }

        if (timed && remainingNanos <= 0) {
          throw new TimeoutException("Unable to acquire lock within the specified time.");
        }

        parent.localLock.lock();
        try {

          if ((result.serverLogicalLatestVersion() < parent.clientLogicalLockVersion
                  && parent.clientLogicalLockVersion > 0)
              || parent.clientLogicalLockVersion == 0L) {
            continue Retry;
          }

          while (!this.lockAvailable()) {
            // No need for a local state flag; we just wait for a signal and then re-try.
            if (timed) {
              if (remainingNanos <= 0) throw new TimeoutException("Wait time elapsed.");

              if (!condition.await(remainingNanos, TimeUnit.NANOSECONDS))
                throw new TimeoutException("Wait time elapsed before signal.");

              remainingNanos -= (System.nanoTime() - start);
            } else {
              condition.await();
            }
          }
        } finally {
          parent.localLock.unlock();
        }
      }
    }
  }

  // --- Concrete Lock Implementations ---

  private static class ReadLockImpl extends AbstractLock {
    private ReadLockImpl(DefaultReadWriteLock parent) {
      super(parent);
    }

    @Override
    protected boolean lockAvailable() {
      return parent.state == STATE_AVAILABLE_R || parent.state == STATE_AVAILABLE_RW;
    }

    @Override
    protected Command<LockCommand.AcquireResult> buildAcquireCommand(
        String holderId, long timeout, TimeUnit timeUnit) {
      return new ReadWriteLockCommand.AcquireRead(holderId, parent.leaseId, timeout, timeUnit);
    }

    @Override
    protected Command<Void> buildReleaseCommand(String holderId) {
      return new ReadWriteLockCommand.ReleaseRead(holderId, parent.leaseId);
    }

    @Override
    protected Condition getCondition() {
      return parent.readerCondition;
    }

    @Override
    public boolean isClosed() {
      return parent.isClosed();
    }
  }

  private static class WriteLockImpl extends AbstractLock {
    private WriteLockImpl(DefaultReadWriteLock parent) {
      super(parent);
    }

    @Override
    protected boolean lockAvailable() {
      return parent.state == STATE_AVAILABLE_RW;
    }

    @Override
    protected Command<LockCommand.AcquireResult> buildAcquireCommand(
        String holderId, long timeout, TimeUnit timeUnit) {
      return new ReadWriteLockCommand.AcquireWrite(holderId, parent.leaseId, timeout, timeUnit);
    }

    @Override
    protected Command<Void> buildReleaseCommand(String holderId) {
      return new ReadWriteLockCommand.ReleaseWrite(holderId, parent.leaseId);
    }

    @Override
    protected Condition getCondition() {
      return parent.writerCondition;
    }

    @Override
    public boolean isClosed() {
      return parent.isClosed();
    }
  }
}
