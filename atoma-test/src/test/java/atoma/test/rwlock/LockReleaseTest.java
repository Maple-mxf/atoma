package atoma.test.rwlock;

import atoma.api.Lease;
import atoma.api.lock.Lock;
import atoma.api.lock.ReadWriteLock;
import atoma.test.BaseTest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 测试用例: TEST-REL-001 到 TEST-REL-003 描述: 锁释放相关测试
 *
 * <p>测试目标: TEST-REL-001: 验证客户端主动释放读锁的正确性 TEST-REL-002: 验证客户端主动释放写锁的正确性 TEST-REL-003:
 * 验证读锁全部释放后写锁能够获取
 */
public class LockReleaseTest extends BaseTest {

  @Test
  public void testReadLockRelease() throws Exception {
    final String resourceId = "test-read-release-resource";
    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease.getReadWriteLock(resourceId);
    Lock readLock = readWriteLock.readLock();

    // 获取读锁
    readLock.lock();
    assertThat(readLock.isClosed()).isFalse();

    // 释放读锁
    readLock.unlock();

    // 验证可以重新获取读锁
    readLock.lock();
    try {
      assertThat(readLock.isClosed()).isFalse();
    } finally {
      readLock.unlock();
    }
  }

  @Test
  public void testWriteLockRelease() throws Exception {
    final String resourceId = "test-write-release-resource";
    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease.getReadWriteLock(resourceId);
    Lock writeLock = readWriteLock.writeLock();

    // 获取写锁
    writeLock.lock();
    assertThat(writeLock.isClosed()).isFalse();

    // 释放写锁
    writeLock.unlock();

    // 验证可以重新获取写锁
    writeLock.lock();
    try {
      assertThat(writeLock.isClosed()).isFalse();
    } finally {
      writeLock.unlock();
    }
  }

  @Test
  public void testMultipleReadLocksRelease() throws Exception {
    final String resourceId = "test-multi-read-release-resource";
    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock1 = lease.getReadWriteLock(resourceId);
    ReadWriteLock readWriteLock2 = lease.getReadWriteLock(resourceId);
    ReadWriteLock readWriteLock3 = lease.getReadWriteLock(resourceId);

    Lock readLock1 = readWriteLock1.readLock();
    Lock readLock2 = readWriteLock2.readLock();
    Lock readLock3 = readWriteLock3.readLock();
    Lock writeLock = readWriteLock1.writeLock();

    // 获取三个读锁
    readLock1.lock();
    readLock2.lock();
    readLock3.lock();

    CountDownLatch writeLockAcquired = new CountDownLatch(1);
    AtomicInteger writeLockAttempts = new AtomicInteger(0);

    // 线程尝试获取写锁（应该被阻塞）
    Thread writeThread =
        new Thread(
            () -> {
              try {
                writeLockAttempts.incrementAndGet();
                writeLock.lock(1, TimeUnit.SECONDS);
                writeLockAcquired.countDown();
                writeLock.unlock();
              } catch (Exception e) {
                // 预期行为：在所有读锁释放前无法获取写锁
              }
            });

    writeThread.start();

    // 等待写锁尝试
    Thread.sleep(500);

    // 验证写锁尚未被获取
    assertThat(writeLockAcquired.getCount()).isEqualTo(1);

    // 释放第一个读锁
    readLock1.unlock();

    // 验证写锁仍然被阻塞（还有2个读锁）
    Thread.sleep(500);
    assertThat(writeLockAcquired.getCount()).isEqualTo(1);

    // 释放第二个读锁
    readLock2.unlock();

    // 验证写锁仍然被阻塞（还有1个读锁）
    Thread.sleep(500);
    assertThat(writeLockAcquired.getCount()).isEqualTo(1);

    // 释放最后一个读锁
    readLock3.unlock();

    // 等待写锁获取成功
    writeThread.join();

    // 验证写锁最终成功获取
    assertThat(writeLockAcquired.getCount()).isEqualTo(0);
  }

  @Test
  public void testReentrantReadLockRelease() throws Exception {
    final String resourceId = "test-reentrant-read-release-resource";
    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease.getReadWriteLock(resourceId);
    Lock readLock = readWriteLock.readLock();

    // 重入获取读锁3次
    readLock.lock();
    readLock.lock();
    readLock.lock();

    // 释放读锁3次
    readLock.unlock();
    readLock.unlock();
    readLock.unlock();

    // 现在应该可以获取写锁（所有读锁都已释放）
    Lock writeLock = readWriteLock.writeLock();
    writeLock.lock();
    try {
      assertThat(writeLock.isClosed()).isFalse();
    } finally {
      writeLock.unlock();
    }
  }

  @Test
  public void testReentrantWriteLockRelease() throws Exception {
    final String resourceId = "test-reentrant-write-release-resource";
    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease.getReadWriteLock(resourceId);
    Lock writeLock = readWriteLock.writeLock();

    // 重入获取写锁3次
    writeLock.lock();
    writeLock.lock();
    writeLock.lock();

    // 释放写锁3次
    writeLock.unlock();
    writeLock.unlock();
    writeLock.unlock();

    // 现在应该可以获取新的写锁
    Lock writeLock2 = readWriteLock.writeLock();
    writeLock2.lock();
    try {
      assertThat(writeLock2.isClosed()).isFalse();
    } finally {
      writeLock2.unlock();
    }
  }

  @Test
  public void testLockReleaseNotification() throws Exception {
    final String resourceId = "test-release-notification-resource";
    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease.getReadWriteLock(resourceId);
    Lock writeLock = readWriteLock.writeLock();
    Lock readLock = readWriteLock.readLock();

    // 获取写锁
    writeLock.lock();

    CountDownLatch readLockAcquired = new CountDownLatch(1);
    AtomicInteger waitingThreads = new AtomicInteger(0);

    // 创建多个等待读锁的线程
    for (int i = 0; i < 3; i++) {
      Thread thread =
          new Thread(
              () -> {
                waitingThreads.incrementAndGet();
                try {
                  readLock.lock();
                  readLockAcquired.countDown();
                  Thread.sleep(100); // 保持读锁一段时间
                  readLock.unlock();
                } catch (Exception e) {
                  e.printStackTrace();
                }
              });
      thread.start();
    }

    // 等待所有等待线程启动
    Thread.sleep(500);

    // 释放写锁，应该唤醒所有等待的读线程
    writeLock.unlock();

    // 等待读锁获取通知
    boolean allReadLocksAcquired = readLockAcquired.await(2, TimeUnit.SECONDS);

    // 验证至少有一个读锁被获取
    assertThat(allReadLocksAcquired).isTrue();
  }

  @Test
  public void testReleaseNonExistentLock() {
    final String resourceId = "test-release-non-existent-resource";
    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease.getReadWriteLock(resourceId);
    Lock readLock = readWriteLock.readLock();

    // 尝试释放未持有的锁应该抛出异常
    assertThatThrownBy(() -> readLock.unlock())
        .isInstanceOf(IllegalMonitorStateException.class)
        .hasMessageContaining("does not hold the lock");
  }
}
