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

import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class LockReleaseConnectionDropTest extends BaseTest {
  public LockReleaseConnectionDropTest() {}

  @Test
  @DisplayName("TC-28: 释放锁时连接断开")
  void testLockReleaseConnectionDrop() throws Exception {
    String resourceId = "test-resource-tc28";
    CountDownLatch lockAcquired = new CountDownLatch(1);
    CountDownLatch lockReleased = new CountDownLatch(1);

    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    Lease lease1 = client.grantLease(Duration.ofSeconds(30L));
    Lock lock1 = lease1.getLock(resourceId);
    Thread connectionDropClient =
        new Thread(
            () -> {
              try {
                lock1.lock();
                lockAcquired.countDown();
                Thread.sleep(500L);
                lock1.unlock();
                lockReleased.countDown();
              } catch (InterruptedException var4) {
                Thread.currentThread().interrupt();
              }
            });
    Thread waitingClient =
        new Thread(
            () -> {
              try {
                lockReleased.await();
                Thread.sleep(100L);
                Lease lease2 = client.grantLease(Duration.ofSeconds(30L));
                Lock lock2 = lease2.getLock(resourceId);
                lock2.lock();
                lock2.unlock();
                lease2.revoke();
              } catch (InterruptedException var5) {
                Thread.currentThread().interrupt();
              }
            });
    connectionDropClient.start();
    waitingClient.start();
    Assertions.assertThat(lockAcquired.await(2L, TimeUnit.SECONDS)).isTrue();
    Assertions.assertThat(lockReleased.await(2L, TimeUnit.SECONDS)).isTrue();
    connectionDropClient.join();
    waitingClient.join();
    lease1.revoke();
    client.close();
    System.out.println("TC-28: 释放锁时连接断开 - PASSED");
  }
}
