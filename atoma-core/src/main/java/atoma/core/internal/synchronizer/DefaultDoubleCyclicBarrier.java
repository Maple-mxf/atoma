package atoma.core.internal.synchronizer;

import atoma.api.coordination.CoordinationStore;
import atoma.api.coordination.ResourceChangeEvent;
import atoma.api.coordination.Subscription;
import atoma.api.coordination.command.DoubleCyclicBarrierCommand;
import atoma.api.synchronizer.DoubleCyclicBarrier;
import atoma.core.internal.ThreadUtils;
import com.google.common.annotations.Beta;
import com.google.errorprone.annotations.MustBeClosed;
import com.google.errorprone.annotations.ThreadSafe;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Beta
@ThreadSafe
public class DefaultDoubleCyclicBarrier extends DoubleCyclicBarrier {

  private final String resourceId;
  private final String leaseId;
  private final int parties;
  private final CoordinationStore coordination;
  private final Subscription subscription;

  private final ReentrantLock localLock = new ReentrantLock();
  private final Condition enterCondition = localLock.newCondition();
  private final Condition leaveCondition = localLock.newCondition();

  @MustBeClosed
  public DefaultDoubleCyclicBarrier(
      String resourceId, String leaseId, int parties, CoordinationStore coordination) {
    this.resourceId = resourceId;
    this.leaseId = leaseId;
    this.parties = parties;
    this.coordination = coordination;

    this.subscription =
        coordination.subscribe(
            DoubleCyclicBarrier.class,
            resourceId,
            event -> {
              if (event.getType() == ResourceChangeEvent.EventType.UPDATED) {
                event
                    .getOldNode()
                    .ifPresent(
                        oldNode -> {
                          boolean enterWaitersExisted =
                              oldNode.getData().get("enter_waiters") != null;
                          boolean leaveWaitersExisted =
                              oldNode.getData().get("leave_waiters") != null;

                          event
                              .getNewNode()
                              .ifPresent(
                                  newNode -> {
                                    boolean enterWaitersExists =
                                        newNode.getData().get("enter_waiters") != null;
                                    boolean leaveWaitersExists =
                                        newNode.getData().get("leave_waiters") != null;

                                    localLock.lock();
                                    try {
                                      if (enterWaitersExisted && !enterWaitersExists) {
                                        enterCondition.signalAll();
                                      }
                                      if (leaveWaitersExisted && !leaveWaitersExists) {
                                        leaveCondition.signalAll();
                                      }
                                    } finally {
                                      localLock.unlock();
                                    }
                                  });
                        });
              }
            });
  }

  @Override
  public int getParticipants() {
    return parties;
  }

  @Override
  public void enter() throws InterruptedException {
    String participantId = ThreadUtils.getCurrentHolderId(leaseId);
    var command = new DoubleCyclicBarrierCommand.Enter(parties, participantId, leaseId);

    // Loop to handle optimistic locking failures
    for (; ; ) {
      if (coordination.execute(resourceId, command).passed()) {
        break;
      }
    }

    localLock.lock();
    try {
      // A full implementation would need generation tracking to be robust against spurious wakeups.
      // This simplified version waits for any signal on the enter condition.
      enterCondition.await();
    } finally {
      localLock.unlock();
    }
  }

  @Override
  public void leave() throws InterruptedException {
    String participantId = ThreadUtils.getCurrentHolderId(leaseId);
    var command = new DoubleCyclicBarrierCommand.Leave(parties, participantId, leaseId);

    // Loop to handle optimistic locking failures
    for (; ; ) {
      if (coordination.execute(resourceId, command).passed()) {
        break;
      }
    }

    localLock.lock();
    try {
      // A full implementation would need generation tracking.
      leaveCondition.await();
    } finally {
      localLock.unlock();
    }
  }

  @Override
  public String getResourceId() {
    return resourceId;
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
