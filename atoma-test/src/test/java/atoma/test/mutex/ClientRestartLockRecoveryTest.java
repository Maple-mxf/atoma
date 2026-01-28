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

public class ClientRestartLockRecoveryTest extends BaseTest {
    public ClientRestartLockRecoveryTest() {
    }

    @Test
    @DisplayName("TC-21: 客户端重启后尝试恢复锁")
    void testClientRestartLockRecovery() throws InterruptedException {
        String resourceId = "test-resource-tc21";
        CountDownLatch lockAcquired = new CountDownLatch(1);
        CountDownLatch clientRestarted = new CountDownLatch(1);
        CountDownLatch lockRecovered = new CountDownLatch(1);
        Lease lease1 = this.atomaClient.grantLease(Duration.ofSeconds(30L));
        Lock lock1 = lease1.getLock(resourceId);
        Thread originalClient = new Thread(() -> {
            try {
                lock1.lock();
                lockAcquired.countDown();
                Thread.sleep(1000L);
                lock1.unlock();
            } catch (InterruptedException var3) {
                Thread.currentThread().interrupt();
            }

        });
        Thread restartClient = new Thread(() -> {
            try {
                lockAcquired.await();
                Thread.sleep(1500L);
                clientRestarted.countDown();
                Lease lease2 = this.atomaClient.grantLease(Duration.ofSeconds(30L));
                Lock lock2 = lease2.getLock(resourceId);
                lock2.lock();
                lockRecovered.countDown();
                lock2.unlock();
                lease2.revoke();
            } catch (InterruptedException var7) {
                Thread.currentThread().interrupt();
            }

        });
        originalClient.start();
        restartClient.start();
        Assertions.assertThat(lockAcquired.await(2L, TimeUnit.SECONDS)).isTrue();
        Assertions.assertThat(clientRestarted.await(3L, TimeUnit.SECONDS)).isTrue();
        Assertions.assertThat(lockRecovered.await(3L, TimeUnit.SECONDS)).isTrue();
        originalClient.join();
        restartClient.join();
        lease1.revoke();
        System.out.println("TC-21: 客户端重启后尝试恢复锁 - PASSED");
    }
}
