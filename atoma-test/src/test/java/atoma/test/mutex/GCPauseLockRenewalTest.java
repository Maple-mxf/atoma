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

public class GCPauseLockRenewalTest extends BaseTest {
    public GCPauseLockRenewalTest() {
    }

    @Test
    @DisplayName("TC-20: 客户端GC暂停导致续期失败")
    void testGCPauseLockRenewalFailure() throws InterruptedException {
        String resourceId = "test-resource-tc20";
        CountDownLatch lockAcquired = new CountDownLatch(1);
        CountDownLatch gcSimulated = new CountDownLatch(1);
        CountDownLatch lockRecovered = new CountDownLatch(1);
        Lease lease1 = this.atomaClient.grantLease(Duration.ofSeconds(1L));
        Lock lock1 = lease1.getLock(resourceId);
        Thread gcAffectedClient = new Thread(() -> {
            try {
                lock1.lock();
                lockAcquired.countDown();
                Thread.sleep(2000L);
                gcSimulated.countDown();
            } catch (InterruptedException var4) {
                Thread.currentThread().interrupt();
            }
        });

        Thread recoveryClient = new Thread(() -> {
            try {
                gcSimulated.await();
                Thread.sleep(500L);
                Lease lease2 = this.atomaClient.grantLease(Duration.ofSeconds(30L));
                Lock lock2 = lease2.getLock(resourceId);
                lock2.lock();
                lockRecovered.countDown();
                lock2.unlock();
                lease2.revoke();
            } catch (InterruptedException var6) {
                Thread.currentThread().interrupt();
            }

        });
        gcAffectedClient.start();
        recoveryClient.start();
        Assertions.assertThat(lockAcquired.await(2L, TimeUnit.SECONDS)).isTrue();
        Assertions.assertThat(gcSimulated.await(3L, TimeUnit.SECONDS)).isTrue();
        Assertions.assertThat(lockRecovered.await(5L, TimeUnit.SECONDS)).isTrue();
        gcAffectedClient.join();
        recoveryClient.join();
        lease1.revoke();
        System.out.println("TC-20: 客户端GC暂停导致续期失败 - PASSED");
    }
}
