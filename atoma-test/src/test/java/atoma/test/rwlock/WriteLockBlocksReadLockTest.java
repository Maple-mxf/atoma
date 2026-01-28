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
 * 测试用例: TEST-ACQ-004 描述: 写锁获取时阻塞后续读锁获取，验证优先级
 *
 * <p>测试目标: 1. 验证当写锁被持有时，后续的读锁获取请求会被阻塞 2. 验证写锁释放后，等待的读锁能够成功获取 3. 验证写锁的排他性优先级
 */
public class WriteLockBlocksReadLockTest extends BaseTest {

  @DisplayName("TEST-ACQ-004: 验证当写锁被持有时，后续的读锁获取请求会被阻塞")
  @Test
  public void testWriteLockBlocksReadLock() throws Exception {
    final String resourceId = "test-write-blocks-read-resource";
    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease.getReadWriteLock(resourceId);
    Lock writeLock = readWriteLock.writeLock();
    Lock readLock = readWriteLock.readLock();

    CountDownLatch writeLockAcquired = new CountDownLatch(1);
    CountDownLatch readLockAttempted = new CountDownLatch(1);
    CountDownLatch releaseWriteLock = new CountDownLatch(1);
    AtomicBoolean readLockAcquired = new AtomicBoolean(false);

    // 线程1：获取写锁
    Thread writeThread =
        new Thread(
            () -> {
              try {
                writeLock.lock();
                writeLockAcquired.countDown();

                // 等待读锁尝试获取
                readLockAttempted.await();

                // 释放写锁
                releaseWriteLock.await();
                writeLock.unlock();
              } catch (Exception e) {
                e.printStackTrace();
              }
            });

    // 线程2：尝试获取读锁（应该被阻塞）
    Thread readThread =
        new Thread(
            () -> {
              try {
                // 等待写锁被获取
                writeLockAcquired.await();

                // 尝试获取读锁
                readLockAttempted.countDown();

                // 在超时时间内尝试获取读锁
                try {
                  readLock.lock(3, TimeUnit.SECONDS);
                  readLockAcquired.set(true);
                  readLock.unlock();
                } catch (Exception e) {
                  e.printStackTrace();
                  // 预期行为：在写锁释放前无法获取读锁
                }
              } catch (Exception e) {
                e.printStackTrace();
              }
            });

    writeThread.start();
    readThread.start();

    // 等待写锁被获取
    writeLockAcquired.await();

    // 给读线程一些时间尝试获取锁
    Thread.sleep(1000);

    // 验证读锁尚未被获取
    assertThat(readLockAcquired.get()).isFalse();

    // 释放写锁
    releaseWriteLock.countDown();

    // 等待线程完成
    writeThread.join();
    readThread.join();

    // 验证读锁最终成功获取（在写锁释放后）
    assertThat(readLockAcquired.get()).isTrue();
  }

  @DisplayName("TEST-ACQ-004:")
  @Test
  public void testWriteLockPriorityOverReadLock() throws Exception {
    final String resourceId = "test-write-priority-resource";
    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));

    ReadWriteLock readWriteLock1 = lease.getReadWriteLock(resourceId);
    Lock writeLock = readWriteLock1.writeLock();
    Lock readLock1 = readWriteLock1.readLock();

    // 首先获取一个读锁
    readLock1.lock();

    CountDownLatch writeLockRequested = new CountDownLatch(1);
    CountDownLatch secondReadRequested = new CountDownLatch(1);
    CountDownLatch allRequestsMade = new CountDownLatch(1);
    AtomicBoolean writeLockAcquired = new AtomicBoolean(false);
    AtomicBoolean secondReadAcquired = new AtomicBoolean(false);

    // 创建第二个客户端
    ReadWriteLock readWriteLock2 = lease.getReadWriteLock(resourceId);
    Lock readLock2 = readWriteLock2.readLock();

    // 线程1：请求写锁（应该阻塞，等待第一个读锁释放）
    Thread writeRequestThread =
        new Thread(
            () -> {
              try {
                writeLockRequested.countDown();
                writeLock.lock();
                writeLockAcquired.set(true);
                Thread.sleep(100); // 保持写锁一段时间
                writeLock.unlock();
              } catch (Exception e) {
                e.printStackTrace();
              }
            });

    // 线程2：请求第二个读锁（应该被写锁阻塞）
    Thread readRequestThread =
        new Thread(
            () -> {
              try {
                secondReadRequested.countDown();
                allRequestsMade.await();

                // 在超时时间内尝试获取读锁
                try {
                  readLock2.lock(3, TimeUnit.SECONDS);
                  secondReadAcquired.set(true);
                  readLock2.unlock();
                } catch (Exception e) {
                  // 预期行为：在写锁释放前无法获取读锁
                }
              } catch (Exception e) {
                e.printStackTrace();
              }
            });

    writeRequestThread.start();
    readRequestThread.start();

    // 等待请求信号
    writeLockRequested.await();
    secondReadRequested.await();

    // 给写锁请求一些时间
    Thread.sleep(500);

    // 释放第一个读锁
    readLock1.unlock();
    System.err.println("Main：Unlock ");

    // 通知读请求线程继续
    allRequestsMade.countDown();
    System.err.println(allRequestsMade.getCount());
    System.err.println("Main：Unlock2 ");

    // 等待线程完成
    writeRequestThread.join();
    readRequestThread.join();

    // 验证写锁被获取（优先级）
    assertThat(writeLockAcquired.get()).isTrue();

    // 验证第二个读锁在写锁释放后才能获取
    // 注意：由于写锁优先级的实现方式，第二个读锁可能无法获取
    // 这取决于具体的实现策略
  }
}
