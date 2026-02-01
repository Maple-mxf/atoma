//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package atoma.test.mutex;

import atoma.api.Lease;
import atoma.api.lock.Lock;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import atoma.client.AtomaClient;
import atoma.test.BaseTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class NonOwnerLockReleaseTest extends BaseTest {
  public NonOwnerLockReleaseTest() {}

  @Test
  @DisplayName("TC-06: 非持有者尝试释放锁（应失败）")
  void testNonOwnerLockRelease() throws Exception {
    String resourceId = "test-resource-tc06";
    CountDownLatch lockAcquired = new CountDownLatch(1);
    CountDownLatch releaseAttempted = new CountDownLatch(1);
    AtomicBoolean releaseFailed = new AtomicBoolean(false);
    AtomaClient client = new AtomaClient(newMongoCoordinationStore());
    Lease lease1 = client.grantLease(Duration.ofSeconds(30L));
    Lock lock1 = lease1.getLock(resourceId);
    Thread firstThread =
        new Thread(
            () -> {
              try {
                lock1.lock();
                lockAcquired.countDown();
                Thread.sleep(1000L);
              } catch (InterruptedException var6) {
                Thread.currentThread().interrupt();
              } finally {
                lock1.unlock();
              }
            });
    Lease lease2 = client.grantLease(Duration.ofSeconds(30L));
    Lock lock2 = lease2.getLock(resourceId);
    Thread secondThread =
        new Thread(
            () -> {
              try {
                lockAcquired.await();
                Thread.sleep(100L);

                try {
                  lock2.unlock();
                } catch (IllegalMonitorStateException var5) {
                  releaseFailed.set(true);
                }

                releaseAttempted.countDown();
              } catch (InterruptedException var6) {
                Thread.currentThread().interrupt();
              }
            });
    firstThread.start();
    secondThread.start();
    Assertions.assertThat(lockAcquired.await(2L, TimeUnit.SECONDS)).isTrue();
    Assertions.assertThat(releaseAttempted.await(2L, TimeUnit.SECONDS)).isTrue();
    Assertions.assertThat(releaseFailed.get()).isTrue();
    firstThread.join();
    secondThread.join();
    lease1.revoke();
    lease2.revoke();
    client.close();
    System.out.println("TC-06: 非持有者尝试释放锁（应失败） - PASSED");
  }
}
