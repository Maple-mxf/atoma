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

public class NetworkReconnectionLockTest extends BaseTest {
    public NetworkReconnectionLockTest() {
    }

    @Test
    @DisplayName("TC-29: 网络闪断后的重连机制")
    void testNetworkReconnectionAfterFlashDisconnect() throws InterruptedException {
        String resourceId = "test-resource-tc29";
        CountDownLatch lockAcquired = new CountDownLatch(1);
        CountDownLatch networkFlashed = new CountDownLatch(1);
        CountDownLatch reconnected = new CountDownLatch(1);
        Lease lease1 = this.atomaClient.grantLease(Duration.ofSeconds(30L));
        Lock lock1 = lease1.getLock(resourceId);
        Thread firstClient = new Thread(() -> {
            try {
                lock1.lock();
                lockAcquired.countDown();
                Thread.sleep(500L);
                lock1.unlock();
                networkFlashed.countDown();
            } catch (InterruptedException var4) {
                Thread.currentThread().interrupt();
            }

        });
        Thread reconnectionClient = new Thread(() -> {
            try {
                networkFlashed.await();
                Thread.sleep(100L);
                Lease lease2 = this.atomaClient.grantLease(Duration.ofSeconds(30L));
                Lock lock2 = lease2.getLock(resourceId);
                lock2.lock();
                reconnected.countDown();
                lock2.unlock();
                lease2.revoke();
            } catch (InterruptedException var6) {
                Thread.currentThread().interrupt();
            }

        });
        firstClient.start();
        reconnectionClient.start();
        Assertions.assertThat(lockAcquired.await(2L, TimeUnit.SECONDS)).isTrue();
        Assertions.assertThat(networkFlashed.await(2L, TimeUnit.SECONDS)).isTrue();
        Assertions.assertThat(reconnected.await(2L, TimeUnit.SECONDS)).isTrue();
        firstClient.join();
        reconnectionClient.join();
        lease1.revoke();
        System.out.println("TC-29: 网络闪断后的重连机制 - PASSED");
    }
}
