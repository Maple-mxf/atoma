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
 * 测试用例: TEST-ACQ-005
 * 描述: 读锁存在时写锁获取被阻塞，验证互斥性
 *
 * 测试目标:
 * 1. 验证当读锁被持有时，写锁获取请求会被阻塞
 * 2. 验证读锁的共享性不会阻止写锁的排他性需求
 * 3. 验证所有读锁释放后，等待的写锁能够成功获取
 */
public class ReadLockBlocksWriteLockTest extends BaseTest {

    @DisplayName("TEST-ACQ-005: 验证当读锁被持有时，写锁获取请求会被阻塞")
    @Test
    public void testReadLockBlocksWriteLock() throws Exception {
        final String resourceId = "test-read-blocks-write-resource";
        Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
        ReadWriteLock readWriteLock = lease.getReadWriteLock(resourceId);
        Lock readLock = readWriteLock.readLock();
        Lock writeLock = readWriteLock.writeLock();

        // 首先获取读锁
        readLock.lock();

        CountDownLatch writeLockAttempted = new CountDownLatch(1);
        CountDownLatch releaseReadLock = new CountDownLatch(1);
        AtomicBoolean writeLockAcquired = new AtomicBoolean(false);

        // 线程1：尝试获取写锁（应该被阻塞）
        Thread writeThread = new Thread(() -> {
            try {
                writeLockAttempted.countDown();

                // 在超时时间内尝试获取写锁
                try {
                    writeLock.lock(2, TimeUnit.SECONDS);
                    writeLockAcquired.set(true);
                    writeLock.unlock();
                } catch (Exception e) {
                    // 预期行为：在读锁释放前无法获取写锁
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        writeThread.start();

        // 等待写锁尝试获取
        writeLockAttempted.await();

        // 给写线程一些时间尝试获取锁
        Thread.sleep(1000);

        // 验证写锁尚未被获取
        assertThat(writeLockAcquired.get()).isFalse();

        // 释放读锁
        readLock.unlock();

        // 等待写线程完成
        writeThread.join();

        // 验证写锁最终成功获取（在读锁释放后）
        assertThat(writeLockAcquired.get()).isTrue();
    }

    @DisplayName("TEST-ACQ-005: 验证当读锁被持有时，写锁获取请求会被阻塞")
    @Test
    public void testMultipleReadLocksBlockWriteLock() throws Exception {
        final String resourceId = "test-multi-read-blocks-write-resource";

        // 创建多个读锁
        Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
        ReadWriteLock readWriteLock1 = lease.getReadWriteLock(resourceId);
        Lock readLock1 = readWriteLock1.readLock();


        ReadWriteLock readWriteLock2 = lease.getReadWriteLock(resourceId);
        Lock readLock2 = readWriteLock2.readLock();

        ReadWriteLock readWriteLock3 = lease.getReadWriteLock(resourceId);
        Lock readLock3 = readWriteLock3.readLock();

        // 获取所有读锁
        readLock1.lock();
        readLock2.lock();
        readLock3.lock();

        CountDownLatch writeLockRequested = new CountDownLatch(1);
        CountDownLatch releaseAllReadLocks = new CountDownLatch(1);
        AtomicBoolean writeLockAcquired = new AtomicBoolean(false);

        // 线程：请求写锁（应该被所有读锁阻塞）
        Thread writeThread = new Thread(() -> {
            try {
                writeLockRequested.countDown();

                ReadWriteLock writeReadWriteLock = lease.getReadWriteLock(resourceId);
                Lock writeLock = writeReadWriteLock.writeLock();

                // 在超时时间内尝试获取写锁
                try {
                    writeLock.lock(3, TimeUnit.SECONDS);
                    writeLockAcquired.set(true);
                    writeLock.unlock();
                } catch (Exception e) {
                    // 预期行为：在所有读锁释放前无法获取写锁
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        writeThread.start();

        // 等待写锁请求
        writeLockRequested.await();

        // 给写线程一些时间尝试获取锁
        Thread.sleep(1000);

        // 验证写锁尚未被获取
        assertThat(writeLockAcquired.get()).isFalse();

        // 释放所有读锁
        readLock3.unlock();
        readLock2.unlock();
        readLock1.unlock();

        // 等待写线程完成
        writeThread.join();

        // 验证写锁最终成功获取（在所有读锁释放后）
        assertThat(writeLockAcquired.get()).isTrue();
    }
}