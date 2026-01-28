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
import java.util.concurrent.TimeoutException;

import atoma.test.BaseTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class LockLeaseDurationTest extends BaseTest {
    public LockLeaseDurationTest() {
    }

    @Test
    @DisplayName("TC-04: 获取锁时指定不同的租约时长")
    void testLockWithDifferentLeaseDurations() throws InterruptedException {
        this.testLeaseDuration(Duration.ofSeconds(1L), "short-lease");
        this.testLeaseDuration(Duration.ofSeconds(5L), "medium-lease");
        this.testLeaseDuration(Duration.ofSeconds(10L), "long-lease");
        System.out.println("TC-04: 获取锁时指定不同的租约时长 - PASSED");
    }

    private void testLeaseDuration(Duration leaseDuration, String resourceSuffix) throws InterruptedException {
        String resourceId = "test-resource-tc04-" + resourceSuffix;
        Lease lease = this.atomaClient.grantLease(leaseDuration);
        Lock lock = lease.getLock(resourceId);
        Assertions.assertThat(lease.getTtlDuration()).isEqualTo(leaseDuration);
        CountDownLatch latch = new CountDownLatch(1);
        Thread testThread = new Thread(() -> {
            try {
                lock.lock(2L, TimeUnit.SECONDS);
                latch.countDown();
                Thread.sleep(100L);
                lock.unlock();
            } catch (TimeoutException | InterruptedException var3) {
            }

        });
        testThread.start();
        Assertions.assertThat(latch.await(3L, TimeUnit.SECONDS)).isTrue();
        testThread.join();
        lease.revoke();
    }
}
