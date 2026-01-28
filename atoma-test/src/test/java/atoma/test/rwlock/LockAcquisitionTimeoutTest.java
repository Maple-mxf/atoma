package atoma.test.rwlock;

import atoma.api.Lease;
import atoma.api.lock.Lock;
import atoma.api.lock.ReadWriteLock;
import atoma.test.BaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 测试用例: TEST-ACQ-007 描述: 获取锁时设置不同超时时间，验证超时控制
 *
 * <p>测试目标: 1. 验证锁获取操作支持超时参数 2. 验证超时机制的正确性 3. 验证超时异常的处理
 */
public class LockAcquisitionTimeoutTest extends BaseTest {

  @DisplayName("TEST-ACQ-007: 验证锁获取操作支持超时参数")
  @Test
  public void testReadLockAcquisitionTimeout() throws Exception {
    final String resourceId = "test-read-timeout-resource";
    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease.getReadWriteLock(resourceId);
    Lock readLock1 = readWriteLock.readLock();
    Lock readLock2 = readWriteLock.readLock();

    // 获取第一个读锁
    readLock1.lock();

    try {
      // 第二个读锁应该能够立即获取（读锁共享）
      readLock2.lock(1, TimeUnit.SECONDS);
      readLock2.unlock();
    } finally {
      readLock1.unlock();
      lease.close();
    }
  }

  @DisplayName("TEST-ACQ-007: 验证超时机制的正确性")
  @Test
  public void testWriteLockAcquisitionTimeout() throws Exception {
    final String resourceId = "test-write-timeout-resource";
    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease.getReadWriteLock(resourceId);
    Lock writeLock1 = readWriteLock.writeLock();
    Lock writeLock2 = readWriteLock.writeLock();

    // 获取第一个写锁
    writeLock1.lock();

    CountDownLatch timeoutOccurred = new CountDownLatch(1);
    AtomicBoolean timeoutSuccess = new AtomicBoolean(false);

    // 在线程中尝试获取第二个写锁（应该超时）
    Thread timeoutThread =
        new Thread(
            () -> {
              try {
                writeLock2.lock(1, TimeUnit.SECONDS);
                // 如果成功获取，说明测试失败
                writeLock2.unlock();
              } catch (TimeoutException e) {
                timeoutSuccess.set(true);
                timeoutOccurred.countDown();
              } catch (Exception e) {
                // 其他异常
              }
            });

    timeoutThread.start();
    timeoutOccurred.await();
    timeoutThread.join();

    // 验证超时发生
    assertThat(timeoutSuccess.get()).isTrue();

    // 释放第一个写锁
    writeLock1.unlock();
    lease.close();
  }

  @DisplayName("TEST-ACQ-007: 验证超时机制的正确性")
  @Test
  public void testZeroTimeout() throws Exception {
    final String resourceId = "test-zero-timeout-resource";
    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease.getReadWriteLock(resourceId);
    Lock writeLock1 = readWriteLock.writeLock();
    Lock writeLock2 = readWriteLock.writeLock();

    // 获取第一个写锁
    writeLock1.lock();

    try {
      // 尝试用零超时获取第二个写锁（应该立即失败）
      assertThatThrownBy(() -> writeLock2.lock(0, TimeUnit.SECONDS))
          .isInstanceOf(TimeoutException.class);
    } finally {
      writeLock1.unlock();
    }
  }

  @DisplayName("TEST-ACQ-007: 验证超时机制的正确性")
  @Test
  public void testNegativeTimeout() throws Exception {
    final String resourceId = "test-negative-timeout-resource";
    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease.getReadWriteLock(resourceId);
    Lock readLock = readWriteLock.readLock();

    // 负超时应该被视为无限等待（根据实现而定）
    // 这里假设负值会抛出异常或被当作无限等待
    readLock.lock(-1, TimeUnit.SECONDS);

    try {
      assertThat(readLock.isClosed()).isFalse();
    } finally {
      readLock.unlock();
    }
  }

  @DisplayName("TEST-ACQ-007: 验证超时机制的正确性")
  @Test
  public void testVariousTimeoutDurations() throws Exception {
    final String resourceId = "test-various-timeouts-resource";
    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease.getReadWriteLock(resourceId);
    Lock writeLock1 = readWriteLock.writeLock();
    Lock writeLock2 = readWriteLock.writeLock();

    // 获取第一个写锁
    writeLock1.lock();

    try {
      // 测试不同的超时时间
      long[] timeouts = {100, 500, 1000, 2000}; // 毫秒

      for (long timeout : timeouts) {
        long startTime = System.currentTimeMillis();

        try {
          writeLock2.lock(timeout, TimeUnit.MILLISECONDS);
          writeLock2.unlock();
          // 如果成功，验证没有等待太久
          long actualTime = System.currentTimeMillis() - startTime;
          assertThat(actualTime).isLessThan(timeout + 100); // 允许100ms的误差
        } catch (TimeoutException e) {
          // 如果超时，验证等待了大约指定的时间
          long actualTime = System.currentTimeMillis() - startTime;
          assertThat(actualTime).isGreaterThanOrEqualTo(timeout - 100); // 允许100ms的误差
          assertThat(actualTime).isLessThan(timeout + 500); // 允许500ms的上限
        }
      }
    } finally {
      writeLock1.unlock();
    }
  }
}
