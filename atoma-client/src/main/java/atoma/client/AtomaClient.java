package atoma.client;

import atoma.api.AtomaStateException;
import atoma.api.Lease;
import atoma.api.Resourceful;
import atoma.api.coordination.CoordinationStore;
import atoma.api.coordination.command.CleanDeadResourceCommand;
import atoma.api.synchronizer.CountDownLatch;
import atoma.api.synchronizer.CyclicBarrier;
import atoma.api.synchronizer.DoubleCyclicBarrier;
import atoma.core.internal.synchronizer.DefaultCountDownLatch;
import atoma.core.internal.synchronizer.DefaultCyclicBarrier;
import atoma.core.internal.synchronizer.DefaultDoubleCyclicBarrier;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.errorprone.annotations.MustBeClosed;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AtomaClient implements AutoCloseable {
  private final boolean ownExecutor;
  private final CoordinationStore coordinationStore;

  private final Table<Class<? extends Resourceful>, String, Resourceful> atomaResources =
      Tables.synchronizedTable(HashBasedTable.create());

  private final ScheduledExecutorService scheduleExecutor;

  @MustBeClosed
  public AtomaClient(CoordinationStore coordinationStore) {
    this.coordinationStore = coordinationStore;
    this.scheduleExecutor =
        Executors.newScheduledThreadPool(
            8, new ThreadFactoryBuilder().setNameFormat("atoma-ttl-worker-%d").build());
    this.ownExecutor = true;
    startTTLTask();
  }

  public AtomaClient(ScheduledExecutorService ttlExecutor, CoordinationStore coordinationStore) {
    this.coordinationStore = coordinationStore;
    this.scheduleExecutor = ttlExecutor;
    this.ownExecutor = false;
    startTTLTask();
  }

  private void startTTLTask() {
    CleanDeadResourceCommand.Clean cleanCommand = new CleanDeadResourceCommand.Clean(null);
    coordinationStore.execute("", cleanCommand);
    scheduleExecutor.scheduleAtFixedRate(
        () -> {
          try {
            coordinationStore.execute("", cleanCommand);
          } catch (Throwable ignore) {
          }
        },
        0,
        2,
        TimeUnit.SECONDS);
  }

  public Lease grantLease(Duration ttl) {
    Lease lease =
        new DefaultLease(
            scheduleExecutor,
            coordinationStore,
            ttl,
            (t) -> {
              DefaultLease removedLease =
                  (DefaultLease) atomaResources.remove(Lease.class, t.getResourceId());

              if (removedLease != null) {
                CleanDeadResourceCommand.Clean cleanCommand =
                    new CleanDeadResourceCommand.Clean(removedLease.getResourceId());
                coordinationStore.execute("", cleanCommand);
              }
            });
    this.atomaResources.put(Lease.class, lease.getResourceId(), lease);
    return lease;
  }

  @MustBeClosed
  public CountDownLatch getCountDownLatch(String resourceId, int count) {
    CountDownLatch countDownLatch =
        (CountDownLatch) atomaResources.get(CountDownLatch.class, resourceId);
    if (countDownLatch == null) {
      countDownLatch = new DefaultCountDownLatch(resourceId, count, this.coordinationStore);
      atomaResources.put(CountDownLatch.class, resourceId, countDownLatch);
    }
    return countDownLatch;
  }

  public CyclicBarrier getCyclicBarrier(String resourceId, Lease lease, int parties) {
    CyclicBarrier barrier = (CyclicBarrier) atomaResources.get(CyclicBarrier.class, resourceId);
    if (barrier == null) {
      barrier =
          new DefaultCyclicBarrier(
              resourceId, lease.getResourceId(), parties, this.coordinationStore);
      atomaResources.put(CyclicBarrier.class, resourceId, barrier);
    }
    return barrier;
  }

  public DoubleCyclicBarrier getDoubleCyclicBarrier(String resourceId, Lease lease, int parties) {
    DoubleCyclicBarrier doubleBarrier =
        (DoubleCyclicBarrier) atomaResources.get(DoubleCyclicBarrier.class, resourceId);
    if (doubleBarrier == null) {
      doubleBarrier =
          new DefaultDoubleCyclicBarrier(
              resourceId, lease.getResourceId(), parties, this.coordinationStore);
      atomaResources.put(DoubleCyclicBarrier.class, resourceId, doubleBarrier);
    }
    return doubleBarrier;
  }

  @Override
  public synchronized void close() throws Exception {
    if (ownExecutor) {
      scheduleExecutor.shutdown();
    }

    // Avoid ConcurrentModifyException
    HashBasedTable<Class<? extends Resourceful>, String, Resourceful> atomaResoucesCopier =
        HashBasedTable.create(atomaResources);
    atomaResoucesCopier
        .values()
        .forEach(
            resourceful -> {
              try {
                // Which will be modify atomaResources
                resourceful.close();
              } catch (Exception e) {
                throw new AtomaStateException(e);
              }
            });
  }
}
