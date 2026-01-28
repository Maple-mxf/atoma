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
import java.util.concurrent.atomic.AtomicInteger;

import atoma.test.BaseTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class HighConcurrencyLockCompetitionTest extends BaseTest {
    public HighConcurrencyLockCompetitionTest() {
    }

    @Test
    @DisplayName("TC-12: 50+客户端同时竞争同一把锁")
    void testHighConcurrencyLockCompetition() throws InterruptedException {
        String resourceId = "test-resource-tc12";
        int clientCount = 50;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(clientCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger totalAttempts = new AtomicInteger(0);

        for(int i = 0; i < clientCount; ++i) {
            int finalI = i;
            (new Thread(() -> {
                try {
                    startLatch.await();
                    Lease lease = this.atomaClient.grantLease(Duration.ofSeconds(30L));
                    Lock lock = lease.getLock(resourceId);
                    totalAttempts.incrementAndGet();

                    try {
                        lock.lock();
                        successCount.incrementAndGet();
                        Thread.sleep(10L);
                    } catch (InterruptedException var28) {
                        Thread.currentThread().interrupt();
                    } finally {
                        lock.unlock();

                        lease.revoke();
                    }
                } catch (Exception var30) {
                    System.err.println("Client " + finalI + " failed: " + var30.getMessage());
                } finally {
                    completionLatch.countDown();
                }

            })).start();
        }

        startLatch.countDown();
        boolean completed = completionLatch.await(30L, TimeUnit.SECONDS);
        Assertions.assertThat(completed).isTrue();
        Assertions.assertThat(successCount.get()).isEqualTo(clientCount);
        Assertions.assertThat(totalAttempts.get()).isEqualTo(clientCount);
        System.out.println("TC-12: 50+客户端同时竞争同一把锁 - PASSED");
        System.out.println("Total attempts: " + totalAttempts.get());
        System.out.println("Successful acquisitions: " + successCount.get());
    }
}
