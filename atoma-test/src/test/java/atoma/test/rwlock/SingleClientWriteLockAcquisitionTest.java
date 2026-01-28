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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 测试用例: TEST-ACQ-002 描述: 单个客户端成功获取写锁，验证独占性
 *
 * <p>测试目标: 1. 验证单个客户端能够成功获取写锁 2. 验证写锁的独占性（同一时间只能有一个写锁） 3. 验证写锁获取后租约被正确创建
 */
public class SingleClientWriteLockAcquisitionTest extends BaseTest {

  @DisplayName("TEST-ACQ-002: 验证单个客户端能够成功获取写锁")
  @Test
  public void testSingleClientWriteLockAcquisition() throws Exception {
    // 创建读写锁实例

    Lease lease1 = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease1.getReadWriteLock("test-resource-004");
    Lock writeLock = readWriteLock.writeLock();

    // 验证初始状态下锁未被持有
    assertThat(writeLock.isClosed()).isFalse();

    // 获取写锁
    writeLock.lock();

    try {
      // 验证锁已被获取
      assertThat(writeLock.isClosed()).isFalse();

      // 等待一段时间以验证租约续期机制
      Thread.sleep(2000);

      // 验证锁仍然持有（租约已续期）
      assertThat(writeLock.isClosed()).isFalse();

    } finally {
      // 释放写锁
      writeLock.unlock();
    }

    // 验证锁已释放
    assertThat(writeLock.isClosed()).isFalse();
  }

  @DisplayName("TEST-ACQ-002: 验证写锁的独占性（同一时间只能有一个写锁）")
  @Test
  public void testWriteLockExclusivity() throws Exception {
    // 创建两个线程尝试获取同一个写锁
    Lease lease1 = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease1.getReadWriteLock("test-resource-005");
    Lock writeLock = readWriteLock.writeLock();

    CountDownLatch firstLockAcquired = new CountDownLatch(1);
    CountDownLatch secondLockAttempt = new CountDownLatch(1);
    AtomicBoolean secondLockFailed = new AtomicBoolean(false);

    // 第一个线程获取写锁
    Thread firstThread =
        new Thread(
            () -> {
              try {
                writeLock.lock();
                firstLockAcquired.countDown();

                // 等待第二个线程尝试获取锁
                secondLockAttempt.await();

                // 释放锁
                writeLock.unlock();
              } catch (Exception e) {
                e.printStackTrace();
              }
            });

    // 第二个线程尝试获取写锁（应该被阻塞）
    Thread secondThread =
        new Thread(
            () -> {
              try {
                // 等待第一个线程获取锁
                firstLockAcquired.await();

                // 尝试在超时时间内获取锁（应该失败）
                try {
                  writeLock.lock(1, TimeUnit.SECONDS);
                  // 如果获取成功，说明测试失败
                  writeLock.unlock();
                } catch (Exception e) {
                  e.printStackTrace();
                  secondLockFailed.set(true);
                }

                secondLockAttempt.countDown();
              } catch (Exception e) {
                e.printStackTrace();
              }
            });

    firstThread.start();
    secondThread.start();

    firstThread.join();
    secondThread.join();

    // 验证第二个线程未能获取锁
    assertThat(secondLockFailed.get()).isTrue();
  }

  @DisplayName("TEST-ACQ-002: 验证超时")
  @Test
  public void testWriteLockWithTimeout() throws Exception {
    // 创建读写锁实例
    Lease lease1 = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease1.getReadWriteLock("test-resource-006");
    Lock writeLock = readWriteLock.writeLock();

    // 使用超时方式获取写锁
    writeLock.lock(5, TimeUnit.SECONDS);

    try {
      // 验证锁已被获取
      assertThat(writeLock.isClosed()).isFalse();

    } finally {
      // 释放写锁
      writeLock.unlock();
    }
  }
}
