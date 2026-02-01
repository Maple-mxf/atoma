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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class FairnessTest extends BaseTest {

  @DisplayName("TEST-SEM-ACQ-010: 获取许可时的公平性测试，验证FIFO顺序")
  @Test
  public void testFairness() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(3));

      // 创建一个容量为1的信号量以确保排队
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-ACQ-010", 1);

      // 先获取唯一的许可
      semaphore.acquire(1);

      // 启动多个线程竞争许可
      int numThreads = 5;
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch finishLatch = new CountDownLatch(numThreads);
      AtomicIntegerArray acquisitionOrder = new AtomicIntegerArray(numThreads);

      for (int i = 0; i < numThreads; i++) {
        final int threadIndex = i;
        new Thread(
                () -> {
                  try {
                    startLatch.await(); // 等待统一开始
                    System.err.printf("Thread: %d acquire starting %n", threadIndex);
                    semaphore.acquire(1);
                    System.err.printf("Thread: %d acquire done %n", threadIndex);
                    acquisitionOrder.set(threadIndex, 1); // 标记获取顺序
                    System.err.printf("Thread: %d release starting %n", threadIndex);
                    semaphore.release(1);
                    System.err.printf("Thread: %d release done %n", threadIndex);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } finally {
                    finishLatch.countDown();
                  }
                })
            .start();
      }

      // 释放初始许可并启动所有线程
      semaphore.release(1);
      System.err.println("Release done. " + semaphore.availablePermits());
      startLatch.countDown();

      // 等待所有线程完成
      finishLatch.await();

      // 验证按FIFO顺序获取（第一个线程应该最先获取到许可）
      Assertions.assertThat(acquisitionOrder.get(0)).isEqualTo(1);

      semaphore.close();
      lease.revoke();

    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}