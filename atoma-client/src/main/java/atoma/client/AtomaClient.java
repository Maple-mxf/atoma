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
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AtomaClient implements AutoCloseable {
  private final CoordinationStore coordinationStore;

  private final Table<Class<? extends Resourceful>, String, Resourceful> atomaResources =
      HashBasedTable.create();

  private final ScheduledExecutorService scheduleExecutor =
      Executors.newScheduledThreadPool(
          8, new ThreadFactoryBuilder().setNameFormat("atoma-ttl-worker-%d").build());

  public AtomaClient(CoordinationStore coordinationStore) {
    this.coordinationStore = coordinationStore;
    scheduleExecutor.scheduleAtFixedRate(
        () -> {
          CleanDeadResourceCommand.Clean command = new CleanDeadResourceCommand.Clean();
          coordinationStore.execute("", command);
        },
        0,
        60,
        TimeUnit.SECONDS);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  for (Resourceful resourceful : atomaResources.values()) {
                    try {
                      resourceful.close();
                    } catch (Exception e) {
                      //e.printStackTrace();
                    }
                  }
                }));
  }

  public synchronized Lease grantLease(Duration ttl) {
    Lease lease =
        new DefaultLease(
            scheduleExecutor,
            coordinationStore,
            ttl,
            (t) -> atomaResources.remove(Lease.class, t.getResourceId()));
    this.atomaResources.put(Lease.class, lease.getResourceId(), lease);
    return lease;
  }

  public synchronized CountDownLatch getCountDownLatch(String resourceId, int count) {
    CountDownLatch countDownLatch =
        (CountDownLatch) atomaResources.get(CountDownLatch.class, resourceId);
    if (countDownLatch == null) {
      countDownLatch = new DefaultCountDownLatch(resourceId, count, this.coordinationStore);
      atomaResources.put(CountDownLatch.class, resourceId, countDownLatch);
    }
    return countDownLatch;
  }

  public synchronized CyclicBarrier getCyclicBarrier(String resourceId, Lease lease, int parties) {
    CyclicBarrier barrier = (CyclicBarrier) atomaResources.get(CyclicBarrier.class, resourceId);
    if (barrier == null) {
      barrier =
          new DefaultCyclicBarrier(
              resourceId, lease.getResourceId(), parties, this.coordinationStore);
      atomaResources.put(CyclicBarrier.class, resourceId, barrier);
    }
    return barrier;
  }

  public synchronized DoubleCyclicBarrier getDoubleCyclicBarrier(
      String resourceId, Lease lease, int parties) {
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
  public void close() throws Exception {
    coordinationStore.close();
    scheduleExecutor.shutdown();
    this.atomaResources
        .values()
        .forEach(
            res -> {
              try {
                res.close();
              } catch (Exception e) {
                throw new AtomaStateException(e);
              }
            });
  }
}
