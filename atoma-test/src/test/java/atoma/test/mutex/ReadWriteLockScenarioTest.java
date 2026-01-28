package atoma.test.mutex;

import atoma.api.Lease;
import atoma.api.lock.Lock;
import atoma.test.BaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test Case: TC-14 Function Scope: 高并发竞争测试 Description: 读写锁场景：多个读锁，独占写锁
 *
 * <p>Note: This test uses regular mutex locks to simulate the scenario since the current
 * implementation doesn't have separate read/write locks
 */
@Deprecated
public class ReadWriteLockScenarioTest extends BaseTest {

  @Test
  @DisplayName("TC-14: 读写锁场景：多个读锁，独占写锁")
  void testReadWriteLockScenario() throws InterruptedException {
    // Given
    String readLockPrefix = "test-read-tc14-";
    String writeLockResource = "test-write-tc14";
    int readerCount = 10;

    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch readersCompleted = new CountDownLatch(readerCount);
    CountDownLatch writerCompleted = new CountDownLatch(1);

    AtomicInteger concurrentReaders = new AtomicInteger(0);
    AtomicInteger maxConcurrentReaders = new AtomicInteger(0);
    AtomicInteger writerAcquired = new AtomicInteger(0);

    // Create reader threads
    for (int i = 0; i < readerCount; i++) {
      final int readerId = i;
      new Thread(
              () -> {
                try {
                  startLatch.await();

                  Lease lease = atomaClient.grantLease(Duration.ofSeconds(30));
                  Lock readLock = lease.getLock(readLockPrefix + readerId);

                  readLock.lock();
                  try {
                    int current = concurrentReaders.incrementAndGet();
                    maxConcurrentReaders.updateAndGet(v -> Math.max(v, current));
                    Thread.sleep(100); // Simulate read operation
                  } finally {
                    concurrentReaders.decrementAndGet();
                    readLock.unlock();
                    lease.revoke();
                    readersCompleted.countDown();
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              })
          .start();
    }

    // Create writer thread
    new Thread(
            () -> {
              try {
                readersCompleted.await(); // Wait for all readers to complete

                Lease lease = atomaClient.grantLease(Duration.ofSeconds(30));
                Lock writeLock = lease.getLock(writeLockResource);

                writeLock.lock();
                try {
                  writerAcquired.incrementAndGet();
                  Thread.sleep(50); // Simulate write operation
                } finally {
                  writeLock.unlock();
                  lease.revoke();
                  writerCompleted.countDown();
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            })
        .start();

    // When
    startLatch.countDown();
    boolean allCompleted = writerCompleted.await(10, TimeUnit.SECONDS);

    // Then
    assertThat(allCompleted).isTrue();
    assertThat(maxConcurrentReaders.get()).isGreaterThanOrEqualTo(1);
    assertThat(writerAcquired.get()).isEqualTo(1);

    System.out.println("TC-14: 读写锁场景：多个读锁，独占写锁 - PASSED");
    System.out.println("Max concurrent readers: " + maxConcurrentReaders.get());
    System.out.println("Writer acquired: " + writerAcquired.get());
  }
}
