package atoma.core.internal.synchronizer;

import atoma.api.AtomaException;
import atoma.api.coordination.CoordinationStore;
import atoma.api.coordination.ResourceChangeEvent;
import atoma.api.coordination.Subscription;
import atoma.api.coordination.command.CyclicBarrierCommand;
import atoma.api.BrokenBarrierException;
import atoma.api.synchronizer.CyclicBarrier;
import atoma.core.internal.ThreadUtils;
import com.google.common.annotations.Beta;
import com.google.errorprone.annotations.MustBeClosed;
import com.google.errorprone.annotations.ThreadSafe;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The default client-side implementation of a distributed {@link CyclicBarrier}.
 *
 * <p>This class provides a distributed synchronization aid that allows a set of processes and
 * threads to all wait for each other to reach a common barrier point. The barrier is "cyclic"
 * because it can be reused after the waiting threads are released.
 *
 * <h3>Implementation Overview</h3>
 *
 * <p>This implementation coordinates state via a backend {@link CoordinationStore}. The core logic
 * of the {@link #await()} method is divided into two distinct phases to ensure correctness under
 * high concurrency.
 *
 * <ol>
 *   <li><b>Command Phase:</b> When a thread calls {@code await()}, the client first enters a
 *       spin-loop, repeatedly sending an {@code Await} command to the coordination store. The
 *       backend command handler uses optimistic locking to atomically register the participant. The
 *       client continues sending the command until the backend confirms successful registration for
 *       the current barrier generation. This phase robustly handles the race conditions of many
 *       clients trying to arrive at the barrier simultaneously.
 *   <li><b>Local Waiting Phase:</b> Once participation is confirmed by the backend, the thread
 *       waits locally on a {@link java.util.concurrent.locks.Condition}. It remains in this waiting
 *       state until notified of a change in the barrier's generation.
 * </ol>
 *
 * <h4>State Tracking and Wake-up Mechanism</h4>
 *
 * <p>The client subscribes to changes on the barrier's state in the coordination store. When the
 * final participant arrives at the barrier, the backend command "trips" it by incrementing a global
 * {@code generation} number. The client's subscription listener detects this change, compares it to
 * its cached generation value, and if the remote generation is newer, it signals the local
 * condition, waking up all waiting threads.
 *
 * <p>If a thread's wait times out, it assumes responsibility for breaking the barrier for all other
 * participants by issuing a {@link #reset()} command.
 *
 * <p><b>Resource Management:</b> This class implements {@link AutoCloseable}. It is crucial to
 * close the barrier instance (e.g., using a try-with-resources block) to release the underlying
 * network subscription and prevent resource leaks.
 *
 * @see atoma.api.synchronizer.CyclicBarrier
 * @see atoma.api.coordination.CoordinationStore
 */
@Beta
@ThreadSafe
public class DefaultCyclicBarrier extends CyclicBarrier {

  private final String resourceId;
  private final String leaseId;
  private final int parties;
  private final CoordinationStore coordination;
  private final Subscription subscription;

  private final ReentrantLock localLock = new ReentrantLock();
  private final Condition generationChanged = localLock.newCondition();
  private final AtomicLong remoteGeneration = new AtomicLong(-1);

  /**
   * Constructs a new DefaultCyclicBarrier.
   *
   * <p>Upon construction, this client immediately communicates with the {@link CoordinationStore}
   * to create the barrier resource if it doesn't exist, validate the number of parties if it does,
   * and fetch the initial generation number. It also establishes a long-lived subscription to
   * listen for changes to the barrier's state.
   *
   * <p>The subscription listener monitors changes to the remote {@code generation} field. When it
   * detects that the generation has advanced (meaning the barrier was tripped or reset), it signals
   * all local threads waiting in the {@link #await()} method.
   *
   * @param resourceId The unique identifier for the distributed barrier resource.
   * @param leaseId The lease ID of the client, used for participant tracking.
   * @param parties The number of parties that must invoke {@link #await()} before the barrier is
   *     tripped.
   * @param coordination The coordination store used for state management and eventing.
   * @throws IllegalArgumentException if a barrier with the same {@code resourceId} already exists
   *     but was initialized with a different number of parties.
   */
  @MustBeClosed
  public DefaultCyclicBarrier(
      String resourceId, String leaseId, int parties, CoordinationStore coordination) {
    if (parties <= 0) {
      throw new IllegalArgumentException("Parties must be a positive number.");
    }
    this.resourceId = resourceId;
    this.leaseId = leaseId;
    this.parties = parties;
    this.coordination = coordination;

    // Get or create the initial state and validate parties.
    var initializeCommand =
        new CyclicBarrierCommand.Await(parties, ThreadUtils.getCurrentHolderId(leaseId), leaseId);
    CyclicBarrierCommand.AwaitResult initResult =
        coordination.execute(resourceId, initializeCommand);
    if (initResult.broken()) {
      throw new IllegalStateException("Barrier was created in a broken state.");
    }

    CyclicBarrierCommand.GetStateResult initialState =
        coordination.execute(resourceId, new CyclicBarrierCommand.GetState());
    if (initialState.parties() > 0 && initialState.parties() != parties) {
      throw new IllegalArgumentException(
          "A barrier with the same ID already exists but with a different number of parties. "
              + "Expected: "
              + parties
              + ", Found: "
              + initialState.parties());
    }
    this.remoteGeneration.set(initialState.generation());

    this.subscription =
        coordination.subscribe(
            CyclicBarrier.class,
            resourceId,
            event -> {
              if (event.getType() == ResourceChangeEvent.EventType.UPDATED) {
                event
                    .getNewNode()
                    .ifPresent(
                        newNode -> {
                          Long newGen = newNode.get("generation");
                          if (newGen != null && newGen > remoteGeneration.get()) {
                            remoteGeneration.set(newGen);
                            localLock.lock();
                            try {
                              generationChanged.signalAll();
                            } finally {
                              localLock.unlock();
                            }
                          }
                        });
              }
            });
  }

  /**
   * Waits until all parties have invoked {@code await} on this barrier.
   *
   * <p>If the current thread is not the last to arrive, it is disabled for thread scheduling
   * purposes and lies dormant until one of the following happens:
   *
   * <ul>
   *   <li>The last party arrives;
   *   <li>Some other thread interrupts the current thread;
   *   <li>Some other thread interrupts one of the other waiting parties;
   *   <li>Some other thread times out while waiting for the barrier;
   *   <li>Some other thread invokes {@link #reset()} on this barrier.
   * </ul>
   *
   * @throws InterruptedException if the current thread was interrupted while waiting.
   * @throws BrokenBarrierException if another thread was interrupted or timed out while the current
   *     thread was waiting, or the barrier was reset.
   */
  @Override
  public void await() throws InterruptedException, BrokenBarrierException {
    try {
      doAwait(null, null);
    } catch (TimeoutException e) {
      throw new AssertionError("Timeout in non-timed await", e);
    }
  }

  /**
   * Waits until all parties have invoked {@code await} on this barrier, or the specified waiting
   * time elapses.
   *
   * <p>If the current thread is not the last to arrive, it is disabled for thread scheduling
   * purposes and lies dormant until one of the events listed for {@link #await()} occurs, or the
   * specified timeout elapses. If the timeout occurs, this thread will attempt to break the barrier
   * for all other participants.
   *
   * @param timeout the time to wait for the barrier
   * @param unit the time unit of the timeout parameter
   * @throws InterruptedException if the current thread was interrupted while waiting.
   * @throws BrokenBarrierException if another thread was interrupted or timed out while the current
   *     thread was waiting, or the barrier was reset.
   * @throws TimeoutException if the specified timeout elapses. On timeout, the barrier is broken.
   */
  @Override
  public void await(long timeout, TimeUnit unit)
      throws InterruptedException, BrokenBarrierException, TimeoutException {
    Objects.requireNonNull(unit);
    doAwait(timeout, unit);
  }

  private void doAwait(Long waitTime, TimeUnit timeUnit)
      throws InterruptedException, BrokenBarrierException, TimeoutException {
    String participantId = ThreadUtils.getCurrentHolderId(leaseId);
    var awaitCommand = new CyclicBarrierCommand.Await(parties, participantId, leaseId);

    // Loop to handle optimistic locking failures. The command is retried if it fails due to a
    // concurrent modification, indicated by a non-passing, non-broken result.
    for (; ; ) {
      try {
        CyclicBarrierCommand.AwaitResult result = coordination.execute(resourceId, awaitCommand);
        if (result.broken()) {
          throw new BrokenBarrierException("The barrier is in a broken state.");
        }
        if (result.passed()) {
          // Our command was successfully processed (we either joined or tripped the barrier).
          // Now, we must wait for the generation to change.
          break;
        }
        // If not passed and not broken, it means a concurrent modification occurred (optimistic
        // lock failure).
        // The loop will immediately retry the command.
      } catch (AtomaException e) {
        // For other errors, wrap and rethrow.
        throw new RuntimeException(
            "Failed to execute await command due to a coordination error", e);
      }
    }

    long localGen = remoteGeneration.get();
    localLock.lock();
    try {
      while (localGen == remoteGeneration.get()) {
        if (isBroken()) { // Check if barrier was broken by a reset while we were about to wait
          throw new BrokenBarrierException("The barrier was broken while waiting.");
        }
        if (waitTime != null && timeUnit != null) {
          if (!generationChanged.await(waitTime, timeUnit)) {
            reset(); // Break the barrier for others if this thread times out.
            throw new TimeoutException("Wait for barrier to trip timed out.");
          }
        } else {
          generationChanged.await();
        }
      }
    } finally {
      localLock.unlock();
    }
  }

  /**
   * Resets the barrier to its initial state. If any parties are waiting at the barrier when this
   * method is called, they will return with a {@link BrokenBarrierException}.
   *
   * <p>This operation uses optimistic locking to ensure it is applied to the expected barrier
   * state, preventing race conditions with concurrent {@code await} or {@code reset} calls.
   */
  @Override
  public void reset() {
    CyclicBarrierCommand.GetStateResult currentState =
        coordination.execute(resourceId, new CyclicBarrierCommand.GetState());
    coordination.execute(resourceId, new CyclicBarrierCommand.Reset(currentState.version()));
  }

  @Override
  public boolean isBroken() {
    return coordination.execute(resourceId, new CyclicBarrierCommand.GetState()).isBroken();
  }

  @Override
  public int getParties() {
    return parties;
  }

  @Override
  public int getNumberWaiting() {
    return coordination.execute(resourceId, new CyclicBarrierCommand.GetState()).numberWaiting();
  }

  @Override
  public String getResourceId() {
    return resourceId;
  }

  /**
   * Closes this barrier and releases any underlying resources, such as the network subscription for
   * listening to state changes. Failure to close the barrier will result in resource leaks.
   */
  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      if (this.subscription != null) {
        this.subscription.close();
      }
    }
  }
}
