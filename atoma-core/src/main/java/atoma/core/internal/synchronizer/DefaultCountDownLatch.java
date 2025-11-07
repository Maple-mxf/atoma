package atoma.core.internal.synchronizer;

import atoma.api.coordination.CoordinationStore;
import atoma.api.coordination.Resource;
import atoma.api.coordination.ResourceChangeEvent;
import atoma.api.coordination.Subscription;
import atoma.api.coordination.command.CountDownLatchCommand;
import atoma.api.synchronizer.CountDownLatch;

import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The default client-side implementation of a distributed {@link CountDownLatch}.
 *
 * <p>This class provides the functionality of a traditional {@code CountDownLatch} for a
 * distributed environment, coordinated via a backend {@link CoordinationStore}. It allows threads
 * to wait until a certain number of operations being performed in other processes are completed.
 *
 * <p><b>Resource Management:</b> This class implements {@link AutoCloseable}. It is crucial to
 * close the latch instance when it is no longer needed (e.g., using a try-with-resources block) to
 * release the underlying network subscription and prevent resource leaks. An explicit {@link
 * #destroy()} method is also provided for manual cleanup of the backend resource.
 *
 * <h4>Implementation Details</h4>
 *
 * The client uses an optimistic-check-then-wait pattern. The {@link #await()} method first checks
 * the count, and only enters a wait state if the count is greater than zero. A background {@link
 * Subscription} listens for changes to the latch on the server. The listener is "smart": it only
 * wakes up waiting threads (via {@code signalAll}) when the latch's count actually reaches zero,
 * preventing unnecessary wake-ups and network traffic.
 *
 * @see atoma.api.synchronizer.CountDownLatch
 */
public class DefaultCountDownLatch implements CountDownLatch, AutoCloseable {

  private final String resourceId;
  private final CoordinationStore coordination;
  private final Subscription subscription;

  private final ReentrantLock localLock = new ReentrantLock();
  private final Condition latchZero = localLock.newCondition();

  /**
   * Constructs a new DefaultCountDownLatch client.
   *
   * <p>This constructor atomically initializes the latch with the given count on the backend if it
   * does not already exist. It also establishes a long-lived subscription to the resource's change
   * stream.
   *
   * @param resourceId The unique ID of the latch resource.
   * @param count The number of times {@link #countDown} must be invoked before threads can pass
   *     through {@link #await()}.
   * @param coordination The coordination store used to execute commands and listen for events.
   */
  public DefaultCountDownLatch(String resourceId, int count, CoordinationStore coordination) {
    this.resourceId = resourceId;
    this.coordination = coordination;

    // Atomically initialize the latch on the server if it doesn't exist.
    coordination.execute(resourceId, new CountDownLatchCommand.Initialize(count));

    this.subscription =
        coordination.subscribe(
            "", // TODO: This should be a defined constant for CountDownLatch
            resourceId,
            event -> {
              boolean shouldSignal = false;
              if (event.getType() == ResourceChangeEvent.EventType.DELETED) {
                shouldSignal = true;
              } else if (event.getType() == ResourceChangeEvent.EventType.UPDATED) {
                shouldSignal =
                    event
                        .getNewNode()
                        .map(node -> (Integer) node.get("count"))
                        .map(c -> c <= 0)
                        .orElse(false);
              }

              if (shouldSignal) {
                signalAllWaiters();
              }
            });
  }

  private void signalAllWaiters() {
    localLock.lock();
    try {
      latchZero.signalAll();
    } finally {
      localLock.unlock();
    }
  }

  /**
   * Decrements the count of the latch, releasing all waiting threads if the count reaches zero.
   *
   * <p>This is a non-blocking, fire-and-forget operation.
   */
  @Override
  public void countDown() {
    coordination.execute(resourceId, new CountDownLatchCommand.CountDown());
  }

  /**
   * Causes the current thread to wait until the latch has counted down to zero, unless the thread
   * is {@linkplain Thread#interrupt interrupted}.
   *
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  @Override
  public void await() throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }

    // Optimistic check to avoid waiting if the latch is already open.
    if (getCount() <= 0) {
      return;
    }

    localLock.lock();
    try {
      // Re-check state after acquiring lock to prevent race conditions.
      while (getCount() > 0) {
        latchZero.await();
      }
    } finally {
      localLock.unlock();
    }
  }

  /**
   * Returns the current count of the latch.
   *
   * <p>This method performs a network call to fetch the authoritative count from the backend.
   *
   * @return the current count
   */
  @Override
  public int getCount() {
    CountDownLatchCommand.GetCountResult result =
        coordination.execute(resourceId, new CountDownLatchCommand.GetCount());
    return result.count();
  }

  @Override
  public String getResourceId() {
    return resourceId;
  }

  /**
   * Permanently deletes the latch resource from the backend coordination service and closes the
   * local client's subscription.
   */
  @Override
  public void destroy() {
    coordination.execute(resourceId, new CountDownLatchCommand.Destroy());
    // Close the subscription locally after destroying the remote resource.
    close();
  }

  /** Closes this latch client and releases the underlying subscription resource. */
  @Override
  public void close() {
    if (this.subscription != null) {
      this.subscription.close();
    }
  }
}
