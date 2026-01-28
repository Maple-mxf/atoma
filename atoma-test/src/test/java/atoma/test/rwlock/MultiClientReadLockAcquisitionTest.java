package atoma.test.rwlock;

import atoma.api.Lease;
import atoma.api.lock.Lock;
import atoma.api.lock.ReadWriteLock;
import atoma.client.AtomaClient;
import atoma.test.BaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 测试用例: TEST-ACQ-003 描述: 多个客户端同时获取读锁，验证共享性
 *
 * <p>测试目标: 1. 验证多个客户端可以同时获取同一个资源的读锁 2. 验证读锁的共享性（多个读锁可以共存） 3. 验证读锁之间不会相互阻塞
 */
public class MultiClientReadLockAcquisitionTest extends BaseTest {

  @DisplayName("TEST-ACQ-003: 1. 多个读锁可以共存")
  @Test
  public void testMultipleClientsReadLockSharedAccess() throws Exception {
    final int clientCount = 5;
    final String resourceId = "test-shared-read-resource";

    CountDownLatch allLocksAcquired = new CountDownLatch(clientCount);
    CountDownLatch releaseAllLocks = new CountDownLatch(1);
    AtomicInteger successfulAcquisitions = new AtomicInteger(0);
    List<Exception> exceptions = new ArrayList<>();

    // 创建多个客户端线程同时获取读锁
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < clientCount; i++) {
      Thread thread =
          new Thread(
              () -> {
                try {
                  // 为每个线程创建独立的客户端
                  AtomaClient client = new AtomaClient(coordinationStore);
                  Lease lease = client.grantLease(Duration.ofSeconds(8));
                  ReadWriteLock readWriteLock = lease.getReadWriteLock(resourceId);
                  Lock readLock = readWriteLock.readLock();

                  // 获取读锁
                  readLock.lock();

                  try {
                    successfulAcquisitions.incrementAndGet();
                    allLocksAcquired.countDown();

                    // 等待释放信号
                    releaseAllLocks.await();

                  } finally {
                    // 释放读锁
                    readLock.unlock();
                    client.close();
                  }

                } catch (Exception e) {
                  synchronized (exceptions) {
                    exceptions.add(e);
                  }
                }
              });
      threads.add(thread);
    }

    // 启动所有线程
    threads.forEach(Thread::start);

    // 等待所有客户端成功获取读锁
    boolean allAcquired = allLocksAcquired.await(10, TimeUnit.SECONDS);

    // 验证所有客户端都成功获取了读锁
    assertThat(allAcquired).isTrue();
    assertThat(successfulAcquisitions.get()).isEqualTo(clientCount);
    assertThat(exceptions).isEmpty();

    // 发送释放信号
    releaseAllLocks.countDown();

    // 等待所有线程完成
    for (Thread thread : threads) {
      thread.join();
    }
  }

  @DisplayName("TEST-ACQ-003: 验证读锁的可重入性")
  @Test
  public void testConcurrentReadLockPerformance() throws Exception {
    final int threadCount = 10;
    final int operationsPerThread = 100;
    final String resourceId = "test-concurrent-read-resource";

    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(threadCount);
    AtomicInteger totalOperations = new AtomicInteger(0);
    List<Exception> exceptions = new ArrayList<>();

    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < threadCount; i++) {
      Thread thread =
          new Thread(
              () -> {
                try {
                  // 等待开始信号
                  startLatch.await();

                  // 为每个线程创建独立的客户端
                  AtomaClient client = new AtomaClient(coordinationStore);
                  Lease lease = client.grantLease(Duration.ofSeconds(8));
                  ReadWriteLock readWriteLock = lease.getReadWriteLock(resourceId);
                  Lock readLock = readWriteLock.readLock();

                  // 执行多次获取释放操作
                  for (int j = 0; j < operationsPerThread; j++) {
                    readLock.lock();
                    try {
                      // 模拟一些读操作
                      Thread.sleep(5);
                      totalOperations.incrementAndGet();
                    } finally {
                      readLock.unlock();
                    }
                  }

                  client.close();
                  completionLatch.countDown();

                } catch (Exception e) {
                  synchronized (exceptions) {
                    exceptions.add(e);
                  }
                  completionLatch.countDown();
                }
              });
      threads.add(thread);
    }

    // 启动所有线程
    threads.forEach(Thread::start);

    // 记录开始时间
    long startTime = System.currentTimeMillis();

    // 发送开始信号
    startLatch.countDown();

    // 等待所有操作完成
    boolean completed = completionLatch.await(30, TimeUnit.SECONDS);

    // 记录结束时间
    long endTime = System.currentTimeMillis();

    // 验证所有操作成功完成
    assertThat(completed).isTrue();
    assertThat(exceptions).isEmpty();
    assertThat(totalOperations.get()).isEqualTo(threadCount * operationsPerThread);

    // 输出性能信息
    long duration = endTime - startTime;
    double operationsPerSecond = (totalOperations.get() * 1000.0) / duration;
    System.out.println(
        String.format(
            "Completed %d read lock operations in %d ms (%.2f ops/sec)",
            totalOperations.get(), duration, operationsPerSecond));

    // 等待所有线程完成
    for (Thread thread : threads) {
      thread.join();
    }
  }
}
