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
import java.util.concurrent.TimeoutException;

import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class NetworkTimeoutLockTest extends BaseTest {
  public NetworkTimeoutLockTest() {}

  @Test
  @DisplayName("TC-26: 获取锁时网络超时")
  void testNetworkTimeoutDuringLockAcquisition() throws Exception {
    String resourceId = "test-resource-tc26";
    CountDownLatch lockAcquired = new CountDownLatch(1);
    CountDownLatch timeoutTested = new CountDownLatch(1);

    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    Lease lease1 = client.grantLease(Duration.ofSeconds(30L));
    Lock lock1 = lease1.getLock(resourceId);
    Thread holdingClient =
        new Thread(
            () -> {
              try {
                lock1.lock();
                lockAcquired.countDown();
                Thread.sleep(5000L);
              } catch (InterruptedException var6) {
                Thread.currentThread().interrupt();
              } finally {
                lock1.unlock();
              }
            });
    Thread timeoutClient =
        new Thread(
            () -> {
              try {
                lockAcquired.await();
                Lease lease2 = client.grantLease(Duration.ofSeconds(30L));
                Lock lock2 = lease2.getLock(resourceId);

                try {
                  lock2.lock(1L, TimeUnit.SECONDS);
                } catch (TimeoutException var11) {
                  timeoutTested.countDown();
                } finally {
                  lease2.revoke();
                }
              } catch (InterruptedException var13) {
                Thread.currentThread().interrupt();
              }
            });
    holdingClient.start();
    timeoutClient.start();
    Assertions.assertThat(lockAcquired.await(2L, TimeUnit.SECONDS)).isTrue();
    Assertions.assertThat(timeoutTested.await(3L, TimeUnit.SECONDS)).isTrue();
    holdingClient.join();
    timeoutClient.join();
    lease1.revoke();

    client.close();
    mongoCoordinationStore.close();

    System.out.println("TC-26: 获取锁时网络超时 - PASSED");
  }
}
