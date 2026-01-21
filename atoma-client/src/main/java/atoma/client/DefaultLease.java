package atoma.client;

import atoma.api.AtomaStateException;
import atoma.api.Leasable;
import atoma.api.Lease;
import atoma.api.coordination.CoordinationStore;
import atoma.api.coordination.ResourceChangeEvent;
import atoma.api.coordination.command.LeaseCommand;
import atoma.api.lock.Lock;
import atoma.api.lock.ReadWriteLock;
import atoma.api.synchronizer.Semaphore;
import atoma.core.internal.lock.DefaultMutexLock;
import atoma.core.internal.lock.DefaultReadWriteLock;
import atoma.core.internal.synchronizer.DefaultSemaphore;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** The default implementation for lease. */
class DefaultLease extends Lease {
  private final CoordinationStore coordinationStore;
  private final String id;

  private final Duration ttlDuration;

  private Instant nextExpireTime;

  private final ScheduledFuture<?> future;

  private final Map<String, Leasable> atomaLeasableResources = new ConcurrentHashMap<>();

  private final Consumer<Lease> onRevokeListener;

  DefaultLease(
      ScheduledExecutorService executor,
      CoordinationStore coordinationStore,
      Duration ttlDuration,
      Consumer<Lease> onRevokeListener) {
    this.coordinationStore = coordinationStore;
    this.id = UUID.randomUUID().toString();
    this.ttlDuration = ttlDuration;
    this.onRevokeListener = onRevokeListener;
    LeaseCommand.Grant grantCmd = new LeaseCommand.Grant(id, ttlDuration);

    LeaseCommand.GrantResult grantResult = coordinationStore.execute(id, grantCmd);

    if (!grantResult.success())
      throw new AtomaStateException("Failure to grant lease resource because of unknown reason");

    this.nextExpireTime = grantResult.nextExpireTime();
    this.future =
        executor.scheduleAtFixedRate(
            () -> {
              try {
                LeaseCommand.TimeToLive ttlCmd =
                    new LeaseCommand.TimeToLive(
                        id, nextExpireTime.plusMillis(ttlDuration.toMillis()));
                LeaseCommand.TimeToLiveResult ttlResult = coordinationStore.execute(id, ttlCmd);
                nextExpireTime = ttlResult.nextExpireTime();
              } catch (Exception e) {
                // e.printStackTrace();
              }
            },
            0,
            ttlDuration.toMillis(),
            TimeUnit.MILLISECONDS);

    coordinationStore.subscribe(
        Lease.class,
        id,
        event -> {
          if (ResourceChangeEvent.EventType.DELETED.equals(event.getType())) {
            closeManagementResource();
            revoke();
          }
        });
  }

  private void closeManagementResource() {
    for (Leasable leasable : atomaLeasableResources.values()) {
      try {
        leasable.close();
      } catch (Exception ignored) {
      }
    }
  }

  @Override
  public synchronized Lock getLock(String resourceId) {
    return (Lock)
        atomaLeasableResources.computeIfAbsent(
            resourceId, _key -> new DefaultMutexLock(resourceId, id, coordinationStore));
  }

  @Override
  public synchronized ReadWriteLock getReadWriteLock(String resourceId) {
    return (ReadWriteLock)
        atomaLeasableResources.computeIfAbsent(
            resourceId, _key -> new DefaultReadWriteLock(resourceId, id, coordinationStore));
  }

  @Override
  public Semaphore getSemaphore(String resourceId, int initialPermits) {
    return (Semaphore)
        atomaLeasableResources.computeIfAbsent(
            resourceId,
            _key -> new DefaultSemaphore(resourceId, id, initialPermits, coordinationStore));
  }

  @Override
  public String getResourceId() {
    return id;
  }

  @Override
  public synchronized void revoke() {
    if (closed.compareAndSet(false, true)) {
      closeManagementResource();
      LeaseCommand.Revoke command = new LeaseCommand.Revoke(id);
      coordinationStore.execute(id, command);
      while (!future.isCancelled() && !future.isDone()) {
        try {
          future.cancel(true);
        } catch (Exception ignored) {
        }
      }
      onRevokeListener.accept(this);
    }
  }

  @Override
  public synchronized void timeToLive() {}

  @Override
  public Duration getTtlDuration() {
    return ttlDuration;
  }

  @Override
  public boolean isRevoked() {
    return isClosed();
  }
}
