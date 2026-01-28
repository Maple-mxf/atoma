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

import atoma.test.BaseTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class LockReleaseAvailabilityTest extends BaseTest {
    public LockReleaseAvailabilityTest() {
    }

    @Test
    @DisplayName("TC-05: 持有者主动释放锁，其他客户端可立即获取")
    void testLockReleaseAvailability() throws InterruptedException {
        String resourceId = "test-resource-tc05";
        CountDownLatch firstAcquired = new CountDownLatch(1);
        CountDownLatch firstReleased = new CountDownLatch(1);
        CountDownLatch secondAcquired = new CountDownLatch(1);
        Lease lease1 = this.atomaClient.grantLease(Duration.ofSeconds(30L));
        Lock lock1 = lease1.getLock(resourceId);
        Thread firstThread = new Thread(() -> {
            try {
                lock1.lock();
                firstAcquired.countDown();
                Thread.sleep(500L);
            } catch (InterruptedException var7) {
                Thread.currentThread().interrupt();
            } finally {
                lock1.unlock();
                firstReleased.countDown();
            }

        });
        Lease lease2 = this.atomaClient.grantLease(Duration.ofSeconds(30L));
        Lock lock2 = lease2.getLock(resourceId);
        Thread secondThread = new Thread(() -> {
            try {
                firstAcquired.await();
                firstReleased.await();
                lock2.lock();
                secondAcquired.countDown();
                lock2.unlock();
            } catch (InterruptedException var5) {
                Thread.currentThread().interrupt();
            }

        });
        firstThread.start();
        secondThread.start();
        Assertions.assertThat(firstAcquired.await(2L, TimeUnit.SECONDS)).isTrue();
        Assertions.assertThat(firstReleased.await(2L, TimeUnit.SECONDS)).isTrue();
        Assertions.assertThat(secondAcquired.await(2L, TimeUnit.SECONDS)).isTrue();
        firstThread.join();
        secondThread.join();
        lease1.revoke();
        lease2.revoke();
        System.out.println("TC-05: 持有者主动释放锁，其他客户端可立即获取 - PASSED");
    }
}
