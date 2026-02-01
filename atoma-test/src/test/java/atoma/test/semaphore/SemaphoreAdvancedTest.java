package atoma.test.semaphore;

import atoma.api.Lease;
import atoma.api.lock.ReadWriteLock;
import atoma.api.synchronizer.Semaphore;
import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class SemaphoreAdvancedTest extends BaseTest {

  @DisplayName("TEST-SEM-ADV-001: 跨多个客户端的信号量状态一致性，验证分布式协调")
  @Test
  public void testCrossClientStateConsistency() throws Exception {
    // 创建多个客户端
    int numClients = 3;
    AtomaClient[] clients = new AtomaClient[numClients];
    Lease[] leases = new Lease[numClients];
    Semaphore[] semaphores = new Semaphore[numClients];

    // 初始化所有客户端
    for (int i = 0; i < numClients; i++) {
      MongoCoordinationStore store = newMongoCoordinationStore();
      ScheduledExecutorService executor = newScheduledExecutorService();
      clients[i] = new AtomaClient(executor, store);
      leases[i] = clients[i].grantLease(Duration.ofSeconds(10));
      semaphores[i] = leases[i].getSemaphore("TEST-SEM-ADV-001", 10);
    }

    try {
      // 客户端1获取5个许可
      semaphores[0].acquire(5);

      // 验证所有客户端看到的可用许可数一致
      for (int i = 0; i < numClients; i++) {
        Assertions.assertThat(semaphores[i].availablePermits()).isEqualTo(5);
      }

      // 客户端2获取3个许可
      semaphores[1].acquire(3);

      // 再次验证所有客户端看到的状态一致
      for (int i = 0; i < numClients; i++) {
        Assertions.assertThat(semaphores[i].availablePermits()).isEqualTo(2);
      }

      // 客户端1释放所有许可
      semaphores[0].release(5);

      // 最终验证状态一致性
      for (int i = 0; i < numClients; i++) {
        Assertions.assertThat(semaphores[i].availablePermits()).isEqualTo(7);
      }

    } finally {
      // 清理所有客户端
      for (int i = 0; i < numClients; i++) {
        if (semaphores[i] != null) semaphores[i].close();
        if (leases[i] != null) leases[i].revoke();
        if (clients[i] != null) clients[i].close();
      }
    }
  }

  @DisplayName("TEST-SEM-ADV-002: 长时间运行的压力测试，验证内存和性能")
  @Test
  public void testLongRunningStressTest() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(30));
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-ADV-002", 100);

      // 测试参数
      int iterations = 1000;
      int numThreads = 10;
      ExecutorService executor = Executors.newFixedThreadPool(numThreads);
      CountDownLatch latch = new CountDownLatch(iterations);
      AtomicLong totalAcquireTime = new AtomicLong(0);
      AtomicLong maxAcquireTime = new AtomicLong(0);

      // 记录开始内存使用
      Runtime runtime = Runtime.getRuntime();
      runtime.gc();
      long startMemory = runtime.totalMemory() - runtime.freeMemory();

      long testStartTime = System.currentTimeMillis();

      // 提交压力测试任务
      for (int i = 0; i < iterations; i++) {
        final int taskId = i;
        executor.submit(() -> {
          try {
            long startTime = System.nanoTime();

            // 随机获取1-5个许可
            int permits = ThreadLocalRandom.current().nextInt(1, 6);
            semaphore.acquire(permits);

            // 模拟处理时间
            Thread.sleep(ThreadLocalRandom.current().nextInt(10, 50));

            // 释放许可
            semaphore.release(permits);

            long endTime = System.nanoTime();
            long duration = endTime - startTime;
            totalAcquireTime.addAndGet(duration);
            maxAcquireTime.updateAndGet(max -> Math.max(max, duration));

          } catch (Exception e) {
            e.printStackTrace();
          } finally {
            latch.countDown();
          }
        });
      }

      // 等待所有任务完成
      latch.await();
      long testEndTime = System.currentTimeMillis();

      // 记录结束内存使用
      runtime.gc();
      long endMemory = runtime.totalMemory() - runtime.freeMemory();

      // 计算性能指标
      long totalTime = testEndTime - testStartTime;
      double avgAcquireTime = totalAcquireTime.get() / (double) iterations / 1_000_000; // 转换为毫秒
      double maxAcquireTimeMs = maxAcquireTime.get() / 1_000_000.0;

      // 验证性能指标在合理范围内
      Assertions.assertThat(totalTime).isLessThan(60000); // 总时间小于60秒
      Assertions.assertThat(avgAcquireTime).isLessThan(100); // 平均获取时间小于100ms
      Assertions.assertThat(maxAcquireTimeMs).isLessThan(500); // 最大获取时间小于500ms

      // 验证内存使用没有显著增长
      long memoryGrowth = endMemory - startMemory;
      System.err.println("memoryGrowth: " + memoryGrowth );
      Assertions.assertThat(memoryGrowth).isLessThan(50 * 1024 * 1024); // 内存增长小于50MB

      executor.shutdown();
      semaphore.close();
      lease.revoke();

    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }

  @DisplayName("TEST-SEM-ADV-003: 信号量的监控和统计功能，验证可观测性")
  @Test
  public void testMonitoringAndStatistics() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(5));
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-ADV-003", 10);

      // 进行一些操作以生成统计信息
      semaphore.acquire(3);
      semaphore.acquire(2);
      semaphore.release(1);
      semaphore.acquire(4);
      semaphore.release(5);

      // 验证基本统计信息（假设有这些方法）
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(7);
//      Assertions.assertThat(semaphore.getQueueLength()).isGreaterThanOrEqualTo(0);
//      Assertions.assertThat(semaphore.hasQueuedThreads()).isNotNull();

      // 验证租约信息
      Assertions.assertThat(lease.getResourceId()).isNotNull();
      // Assertions.assertThat(lease.getTTL()).isPositive();

      semaphore.close();
      lease.revoke();

    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }


  @DisplayName("TEST-SEM-ADV-005: 与读写锁的互操作测试，验证组件集成")
  @Test
  public void testInteroperabilityWithReadWriteLock() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(10));

      // 创建信号量和读写锁
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-ADV-005-SEMAPHORE", 5);
      ReadWriteLock rwLock = lease.getReadWriteLock("TEST-SEM-ADV-005-RWLOCK");

      // 获取信号量许可
      semaphore.acquire(3);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(2);

      // 获取读锁
      rwLock.readLock().lock();
      try {
        // 在持有读锁的同时操作信号量
        semaphore.acquire(2);
        Assertions.assertThat(semaphore.availablePermits()).isEqualTo(0);

        // 释放部分许可
        semaphore.release(1);
        Assertions.assertThat(semaphore.availablePermits()).isEqualTo(1);
      } finally {
        rwLock.readLock().unlock();
      }

      // 获取写锁
      rwLock.writeLock().lock();
      try {
        // 在持有写锁的同时操作信号量
        semaphore.release(4);
        Assertions.assertThat(semaphore.availablePermits()).isEqualTo(5);
      } finally {
        rwLock.writeLock().unlock();
      }

      semaphore.close();
      lease.revoke();

    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}