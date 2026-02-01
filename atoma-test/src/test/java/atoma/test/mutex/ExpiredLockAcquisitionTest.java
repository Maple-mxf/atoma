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

import atoma.client.AtomaClient;
import atoma.test.BaseTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.checkerframework.checker.units.qual.C;

public class ExpiredLockAcquisitionTest extends BaseTest {
    public ExpiredLockAcquisitionTest() {
    }

    @Test
    @DisplayName("TC-03: 获取已过期锁（验证锁释放机制）")
    void testExpiredLockAcquisition() throws Exception {
        String resourceId = "test-resource-tc03";

        AtomaClient client = new AtomaClient(newMongoCoordinationStore());

        Lease lease1 = client.grantLease(Duration.ofSeconds(2L));
        Lock lock1 = lease1.getLock(resourceId);
        CountDownLatch firstAcquired = new CountDownLatch(1);
        CountDownLatch secondAcquired = new CountDownLatch(1);
        Thread firstThread = new Thread(() -> {
            try {
                lock1.lock();
                firstAcquired.countDown();
                Thread.sleep(3000L);
            } catch (InterruptedException var12) {
                Thread.currentThread().interrupt();
            } finally {
                try {
                    lock1.unlock();
                } catch (Exception var11) {
                }

                lease1.revoke();
            }

        });
        Thread secondThread = new Thread(() -> {
            try {
                firstAcquired.await();
                Thread.sleep(2500L);
                Lease lease2 = client.grantLease(Duration.ofSeconds(30L));
                Lock lock2 = lease2.getLock(resourceId);
                lock2.lock();
                secondAcquired.countDown();
                lock2.unlock();
                lease2.revoke();
            } catch (InterruptedException var6) {
                Thread.currentThread().interrupt();
            }

        });
        firstThread.start();
        secondThread.start();
        Assertions.assertThat(firstAcquired.await(5L, TimeUnit.SECONDS)).isTrue();
        Assertions.assertThat(secondAcquired.await(5L, TimeUnit.SECONDS)).isTrue();
        firstThread.join();
        secondThread.join();

        client.close();
        System.out.println("TC-03: 获取已过期锁（验证锁释放机制） - PASSED");
    }
}
