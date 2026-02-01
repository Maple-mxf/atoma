//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package atoma.test.mutex;

import atoma.api.Lease;
import atoma.api.lock.Lock;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class LockReleaseContentionTest extends BaseTest {
  public LockReleaseContentionTest() {}

  @Test
  @DisplayName("TC-13: 锁释放瞬间大量客户端争抢")
  void testLockReleaseContention() throws Exception {
    String resourceId = "test-resource-tc13";
    int waitingClientCount = 30;
    CountDownLatch lockHeld = new CountDownLatch(1);
    CountDownLatch lockReleased = new CountDownLatch(1);
    CountDownLatch allAcquired = new CountDownLatch(waitingClientCount);
    AtomicInteger successCount = new AtomicInteger(0);

    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    Lease lease1 = client.grantLease(Duration.ofSeconds(30L));
    Lock lock1 = lease1.getLock(resourceId);
    Thread holderThread =
        new Thread(
            () -> {
              try {
                lock1.lock();
                lockHeld.countDown();
                Thread.sleep(1000L);
              } catch (InterruptedException var7) {
                Thread.currentThread().interrupt();
              } finally {
                lock1.unlock();
                lockReleased.countDown();
              }
            });

    for (int i = 0; i < waitingClientCount; ++i) {
      (new Thread(
              () -> {
                try {
                  lockHeld.await();
                  lockReleased.await();
                  Lease lease = client.grantLease(Duration.ofSeconds(30L));
                  Lock lock = lease.getLock(resourceId);

                  try {
                    lock.lock();
                    successCount.incrementAndGet();
                    Thread.sleep(10L);
                  } catch (InterruptedException var19) {
                    Thread.currentThread().interrupt();
                  } finally {
                    lock.unlock();
                    lease.revoke();
                  }
                } catch (InterruptedException var21) {
                  Thread.currentThread().interrupt();
                } finally {
                  allAcquired.countDown();
                }
              }))
          .start();
    }

    holderThread.start();
    boolean allCompleted = allAcquired.await(10L, TimeUnit.SECONDS);
    Assertions.assertThat(allCompleted).isTrue();
    Assertions.assertThat(successCount.get()).isEqualTo(waitingClientCount);
    holderThread.join();
    lease1.revoke();

    client.close();

    System.out.println("TC-13: 锁释放瞬间大量客户端争抢 - PASSED");
    System.out.println("Total waiting clients: " + waitingClientCount);
    System.out.println("Successful acquisitions: " + successCount.get());
  }
}
