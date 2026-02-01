package atoma.test.semaphore;

import atoma.api.Lease;
import atoma.api.synchronizer.Semaphore;
import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentMultiClientAcquisitionTest extends BaseTest {

  @DisplayName("TEST-SEM-ACQ-006: 并发多个客户端同时获取许可，验证线程安全性")
  @Test
  public void testConcurrentMultiClientAcquisition() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {

      // 使用多线程并发获取许可
      int numThreads = 5;
      int permitsPerThread = 2;
      CountDownLatch latch = new CountDownLatch(numThreads);
      AtomicInteger totalAcquired = new AtomicInteger(0);
      Lease lease = client.grantLease(Duration.ofSeconds(3));
      for (int i = 0; i < numThreads; i++) {
        new Thread(
                () -> {

                  // 创建一个容量为10的信号量
                  Semaphore semaphore = lease.getSemaphore("TEST-SEM-ACQ-006", 10);
                  try {
                    semaphore.acquire(permitsPerThread);
                    totalAcquired.addAndGet(permitsPerThread);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } finally {
                    latch.countDown();
                  }
                })
            .start();
      }

      // 等待所有线程完成
      latch.await();

      // 验证总共获取的许可数
      Assertions.assertThat(totalAcquired.get()).isEqualTo(numThreads * permitsPerThread);

      // 创建一个容量为10的信号量
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-ACQ-006", 10);

      // 验证剩余许可数
      System.err.println(semaphore.availablePermits());
      Assertions.assertThat(semaphore.availablePermits())
          .isEqualTo(10 - (numThreads * permitsPerThread));

      semaphore.close();
      lease.revoke();

    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
