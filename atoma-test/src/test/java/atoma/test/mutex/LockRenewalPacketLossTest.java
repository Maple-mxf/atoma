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

public class LockRenewalPacketLossTest extends BaseTest {
    public LockRenewalPacketLossTest() {
    }

    @Test
    @DisplayName("TC-27: 续期请求网络丢包")
    void testLockRenewalPacketLoss() throws InterruptedException {
        String resourceId = "test-resource-tc27";
        CountDownLatch lockAcquired = new CountDownLatch(1);
        CountDownLatch leaseExpired = new CountDownLatch(1);
        Lease lease1 = this.atomaClient.grantLease(Duration.ofSeconds(1L));
        Lock lock1 = lease1.getLock(resourceId);
        Thread packetLossClient = new Thread(() -> {
            try {
                lock1.lock();
                lockAcquired.countDown();
                Thread.sleep(3000L);
            } catch (InterruptedException var3) {
                Thread.currentThread().interrupt();
            }

        });
        Thread recoveryClient = new Thread(() -> {
            try {
                lockAcquired.await();
                Thread.sleep(2000L);
                leaseExpired.countDown();
                Lease lease2 = this.atomaClient.grantLease(Duration.ofSeconds(30L));
                Lock lock2 = lease2.getLock(resourceId);
                lock2.lock();
                lock2.unlock();
                lease2.revoke();
            } catch (InterruptedException var6) {
                Thread.currentThread().interrupt();
            }

        });
        packetLossClient.start();
        recoveryClient.start();
        Assertions.assertThat(lockAcquired.await(2L, TimeUnit.SECONDS)).isTrue();
        Assertions.assertThat(leaseExpired.await(3L, TimeUnit.SECONDS)).isTrue();
        packetLossClient.join();
        recoveryClient.join();
        lease1.revoke();
        System.out.println("TC-27: 续期请求网络丢包 - PASSED");
    }
}
