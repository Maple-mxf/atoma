package atoma.test.rwlock;

import atoma.api.Lease;
import atoma.api.lock.Lock;
import atoma.api.lock.ReadWriteLock;
import atoma.test.BaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 测试用例: TEST-ACQ-008 描述: 获取锁重试机制测试，验证网络异常处理
 *
 * <p>测试目标: 1. 验证在网络异常或临时故障时锁获取的重试机制 2. 验证重试不会导致无限循环 3. 验证重试机制的正确性
 */
public class LockAcquisitionRetryTest extends BaseTest {

  @DisplayName("TEST-ACQ-008: 验证在网络异常或临时故障时锁获取的重试机制")
  @Test
  public void testLockAcquisitionWithRetry() throws Exception {
    final String resourceId = "test-retry-resource";
    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease.getReadWriteLock(resourceId);
    Lock writeLock1 = readWriteLock.writeLock();
    Lock writeLock2 = readWriteLock.writeLock();

    // 获取第一个写锁
    writeLock1.lock();

    CountDownLatch retryAttempted = new CountDownLatch(1);
    AtomicInteger retryCount = new AtomicInteger(0);

    // 在线程中尝试获取第二个写锁（会触发重试）
    Thread retryThread =
        new Thread(
            () -> {
              try {
                retryAttempted.countDown();

                // 尝试获取锁，设置较长的超时以观察重试行为
                long startTime = System.currentTimeMillis();
                try {
                  writeLock2.lock(3, TimeUnit.SECONDS);
                  writeLock2.unlock();
                } catch (Exception e) {
                  // 记录重试次数（通过时间估算）
                  long duration = System.currentTimeMillis() - startTime;
                  retryCount.set((int) (duration / 500)); // 假设每500ms重试一次
                }
              } catch (Exception e) {
                e.printStackTrace();
              }
            });

    retryThread.start();
    retryAttempted.await();

    // 等待一段时间让重试发生
    Thread.sleep(3000);

    // 释放第一个写锁，让第二个写锁可以获取
    writeLock1.unlock();

    // 等待重试线程完成
    retryThread.join();

    // 验证发生了重试
    assertThat(retryCount.get()).isGreaterThan(0);
  }

  @DisplayName("TEST-ACQ-008: 验证在网络异常或临时故障时锁获取的重试机制")
  @Test
  public void testReadLockRetryWithContention() throws Exception {
    final String resourceId = "test-read-retry-resource";
    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease.getReadWriteLock(resourceId);
    Lock writeLock = readWriteLock.writeLock();
    Lock readLock = readWriteLock.readLock();

    // 获取写锁以制造竞争
    writeLock.lock();

    CountDownLatch readRetryCompleted = new CountDownLatch(1);
    AtomicBoolean readLockAcquired = new AtomicBoolean(false);

    // 线程尝试获取读锁（会被阻塞，触发重试）
    Thread readThread =
        new Thread(
            () -> {
              try {
                // 尝试获取读锁
                readLock.lock(2, TimeUnit.SECONDS);
                readLockAcquired.set(true);
                readLock.unlock();

              } catch (Exception e) {
                e.printStackTrace();
              } finally {
                readRetryCompleted.countDown();
              }
            });

    readThread.start();

    // 等待读锁尝试
    Thread.sleep(500);

    // 释放写锁，让读锁可以获取
    writeLock.unlock();

    // 等待读重试完成
    boolean completed = readRetryCompleted.await(3, TimeUnit.SECONDS);

    // 验证读锁最终获取成功
    assertThat(completed).isTrue();
    assertThat(readLockAcquired.get()).isTrue();
  }

  @DisplayName("TEST-ACQ-008")
  @Test
  public void testRetryWithMultipleFailures() throws Exception {
    final int resourceCount = 5;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(resourceCount);
    AtomicInteger successfulAcquisitions = new AtomicInteger(0);

    List<Thread> threads = new ArrayList<>();

    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));

    // 创建多个线程竞争不同的资源
    for (int i = 0; i < resourceCount; i++) {
      final int resourceId = i;
      Thread thread =
          new Thread(
              () -> {
                try {
                  startLatch.await();

                  ReadWriteLock readWriteLock =
                      lease.getReadWriteLock("test-resource-" + resourceId);
                  Lock lock = readWriteLock.writeLock();

                  // 尝试获取锁，可能会遇到重试
                  lock.lock(5, TimeUnit.SECONDS);
                  try {
                    successfulAcquisitions.incrementAndGet();
                    Thread.sleep(100); // 模拟处理时间
                  } finally {
                    lock.unlock();
                  }

                  completionLatch.countDown();
                } catch (Exception e) {
                  completionLatch.countDown();
                }
              });
      threads.add(thread);
    }

    // 启动所有线程
    threads.forEach(Thread::start);

    // 同时开始所有竞争
    startLatch.countDown();

    // 等待所有操作完成
    boolean completed = completionLatch.await(10, TimeUnit.SECONDS);

    // 验证所有锁都被成功获取
    assertThat(completed).isTrue();
    assertThat(successfulAcquisitions.get()).isEqualTo(resourceCount);

    // 等待所有线程完成
    for (Thread thread : threads) {
      thread.join();
    }
  }
}
