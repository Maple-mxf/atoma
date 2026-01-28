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

public class NetworkPartitionLockTest extends BaseTest {
    public NetworkPartitionLockTest() {
    }

    @Test
    @DisplayName("TC-19: 持有锁的客户端网络分区")
    void testNetworkPartitionLockScenario() throws InterruptedException {
        String resourceId = "test-resource-tc19";
        CountDownLatch lockAcquired = new CountDownLatch(1);
        CountDownLatch partitionDetected = new CountDownLatch(1);
        Lease lease1 = this.atomaClient.grantLease(Duration.ofSeconds(5L));
        Lock lock1 = lease1.getLock(resourceId);
        Thread partitionedClient = new Thread(() -> {
            try {
                lock1.lock();
                lockAcquired.countDown();
                Thread.sleep(2000L);
            } catch (InterruptedException var3) {
                Thread.currentThread().interrupt();
            }
        });
        Thread detectingClient = new Thread(() -> {
            try {
                lockAcquired.await();
                Thread.sleep(6000L);
                Lease lease2 = this.atomaClient.grantLease(Duration.ofSeconds(30L));
                Lock lock2 = lease2.getLock(resourceId);
                lock2.lock();
                partitionDetected.countDown();
                lock2.unlock();
                lease2.revoke();
            } catch (InterruptedException var6) {
                Thread.currentThread().interrupt();
            }
        });
        partitionedClient.start();
        detectingClient.start();
        Assertions.assertThat(lockAcquired.await(2L, TimeUnit.SECONDS)).isTrue();
        Assertions.assertThat(partitionDetected.await(10L, TimeUnit.SECONDS)).isTrue();
        partitionedClient.join();
        detectingClient.join();
        lease1.revoke();
        System.out.println("TC-19: 持有锁的客户端网络分区 - PASSED");
    }
}
