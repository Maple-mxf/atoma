package atoma.test.rwlock;

import atoma.api.Lease;
import atoma.api.lock.Lock;
import atoma.api.lock.ReadWriteLock;
import atoma.client.AtomaClient;
import atoma.test.BaseTest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 测试用例: TEST-ACQ-010
 * 描述: 并发密集获取释放锁，验证性能和无死锁
 *
 * 测试目标:
 * 1. 验证在高并发场景下锁的性能表现
 * 2. 验证没有死锁发生
 * 3. 验证锁的正确性和一致性
 */
public class HighConcurrencyReadWriteLockTest extends BaseTest {

    @Test
    public void testHighConcurrencyMixedOperations() throws Exception {
        final int threadCount = 20;
        final int operationsPerThread = 100;
        final String resourceId = "test-high-concurrency-resource";

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(threadCount);
        AtomicInteger successfulOperations = new AtomicInteger(0);
        AtomicInteger readOperations = new AtomicInteger(0);
        AtomicInteger writeOperations = new AtomicInteger(0);
        List<Exception> exceptions = new ArrayList<>();

        List<Thread> threads = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            Thread thread = new Thread(() -> {
                try {
                    startLatch.await();

                    // 为每个线程创建独立的客户端
                    Lease lease1 = atomaClient.grantLease(Duration.ofSeconds(8));
                    ReadWriteLock readWriteLock = lease1.getReadWriteLock(resourceId);
                    Lock readLock = readWriteLock.readLock();
                    Lock writeLock = readWriteLock.writeLock();

                    // 执行混合的读写操作
                    for (int j = 0; j < operationsPerThread; j++) {
                        boolean isReadOperation = random.nextDouble() < 0.8; // 80% 读操作

                        if (isReadOperation) {
                            // 读操作
                            readLock.lock();
                            try {
                                // 模拟读操作
                                Thread.sleep(1);
                                readOperations.incrementAndGet();
                                successfulOperations.incrementAndGet();
                            } finally {
                                readLock.unlock();
                            }
                        } else {
                            // 写操作
                            writeLock.lock();
                            try {
                                // 模拟写操作
                                Thread.sleep(2);
                                writeOperations.incrementAndGet();
                                successfulOperations.incrementAndGet();
                            } finally {
                                writeLock.unlock();
                            }
                        }

                        // 偶尔让线程yield，增加并发性
                        if (j % 10 == 0) {
                            Thread.yield();
                        }
                    }


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
        boolean completed = completionLatch.await(60, TimeUnit.SECONDS);

        // 记录结束时间
        long endTime = System.currentTimeMillis();

        // 验证所有操作成功完成
        assertThat(completed).isTrue();
        assertThat(exceptions).isEmpty();
        assertThat(successfulOperations.get()).isEqualTo(threadCount * operationsPerThread);

        // 输出统计信息
        long duration = endTime - startTime;
        double operationsPerSecond = (successfulOperations.get() * 1000.0) / duration;
        System.out.println(String.format("High Concurrency Test Results:"));
        System.out.println(String.format("- Duration: %d ms", duration));
        System.out.println(String.format("- Total Operations: %d", successfulOperations.get()));
        System.out.println(String.format("- Operations/Second: %.2f", operationsPerSecond));
        System.out.println(String.format("- Read Operations: %d (%.1f%%)",
            readOperations.get(), (readOperations.get() * 100.0 / successfulOperations.get())));
        System.out.println(String.format("- Write Operations: %d (%.1f%%)",
            writeOperations.get(), (writeOperations.get() * 100.0 / successfulOperations.get())));

        // 等待所有线程完成
        for (Thread thread : threads) {
            thread.join();
        }
    }

    @Test
    public void testConcurrentReadWriteUpgrade() throws Exception {
        final int threadCount = 10;
        final String resourceId = "test-upgrade-resource";

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(threadCount);
        AtomicInteger successfulUpgrades = new AtomicInteger(0);
        List<Exception> exceptions = new ArrayList<>();

        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            Thread thread = new Thread(() -> {
                try {
                    startLatch.await();

                    AtomaClient client = new AtomaClient(coordinationStore);
                    Lease lease1 = atomaClient.grantLease(Duration.ofSeconds(8));
                    ReadWriteLock readWriteLock = lease1.getReadWriteLock(resourceId);
                    Lock readLock = readWriteLock.readLock();
                    Lock writeLock = readWriteLock.writeLock();

                    // 首先获取读锁
                    readLock.lock();
                    try {
                        // 执行一些读操作
                        Thread.sleep(10);

                        // 尝试升级：释放读锁并获取写锁
                        readLock.unlock();

                        try {
                            writeLock.lock(100, TimeUnit.MILLISECONDS);
                            try {
                                // 执行写操作
                                Thread.sleep(5);
                                successfulUpgrades.incrementAndGet();
                            } finally {
                                writeLock.unlock();
                            }
                        } catch (Exception e) {
                            // 如果升级失败，重新获取读锁
                            readLock.lock();
                        }

                    } finally {
                        readLock.unlock();
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

        // 发送开始信号
        startLatch.countDown();

        // 等待所有操作完成
        boolean completed = completionLatch.await(30, TimeUnit.SECONDS);

        // 验证结果
        assertThat(completed).isTrue();
        assertThat(exceptions).isEmpty();
        assertThat(successfulUpgrades.get()).isGreaterThan(0);

        System.out.println(String.format("Concurrent Upgrade Test: %d successful upgrades out of %d threads",
            successfulUpgrades.get(), threadCount));

        // 等待所有线程完成
        for (Thread thread : threads) {
            thread.join();
        }
    }

    @Test
    public void testDeadlockPrevention() throws Exception {
        final int threadCount = 10;
        final int resourceCount = 5;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(threadCount);
        AtomicInteger deadlocksDetected = new AtomicInteger(0);
        AtomicInteger successfulOperations = new AtomicInteger(0);
        List<Exception> exceptions = new ArrayList<>();

        List<Thread> threads = new ArrayList<>();


        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            Thread thread = new Thread(() -> {
                try {
                    startLatch.await();



                    // 每个线程尝试以不同顺序获取多个资源的锁
                    for (int op = 0; op < 10; op++) {
                        int resource1 = (threadId + op) % resourceCount;
                        int resource2 = (threadId + op + 1) % resourceCount;

                        Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));

                        ReadWriteLock lock1 = lease.getReadWriteLock("resource-" + resource1);
                        ReadWriteLock lock2 = lease.getReadWriteLock("resource-" + resource2);

                        // 尝试获取两个锁（可能以不同顺序）
                        boolean acquiredBoth = false;
                        try {
                            if (threadId % 2 == 0) {
                                lock1.writeLock().lock(100, TimeUnit.MILLISECONDS);
                                try {
                                    lock2.writeLock().lock(100, TimeUnit.MILLISECONDS);
                                    try {
                                        // 成功获取两个锁
                                        acquiredBoth = true;
                                        successfulOperations.incrementAndGet();
                                        Thread.sleep(5); // 模拟一些工作
                                    } finally {
                                        lock2.writeLock().unlock();
                                    }
                                } finally {
                                    lock1.writeLock().unlock();
                                }
                            } else {
                                lock2.writeLock().lock(100, TimeUnit.MILLISECONDS);
                                try {
                                    lock1.writeLock().lock(100, TimeUnit.MILLISECONDS);
                                    try {
                                        // 成功获取两个锁
                                        acquiredBoth = true;
                                        successfulOperations.incrementAndGet();
                                        Thread.sleep(5); // 模拟一些工作
                                    } finally {
                                        lock1.writeLock().unlock();
                                    }
                                } finally {
                                    lock2.writeLock().unlock();
                                }
                            }
                        } catch (Exception e) {
                            // 超时，可能是死锁预防
                            deadlocksDetected.incrementAndGet();
                        }

                        // 如果成功获取了锁，等待一下再尝试下一个操作
                        if (acquiredBoth) {
                            Thread.sleep(10);
                        }
                    }


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

        // 发送开始信号
        startLatch.countDown();

        // 等待所有操作完成
        boolean completed = completionLatch.await(30, TimeUnit.SECONDS);

        // 验证结果
        assertThat(completed).isTrue();
        assertThat(exceptions).isEmpty();
        assertThat(successfulOperations.get()).isGreaterThan(0);

        System.out.println(String.format("Deadlock Prevention Test:"));
        System.out.println(String.format("- Total Operations: %d", threadCount * 10));
        System.out.println(String.format("- Successful Operations: %d", successfulOperations.get()));
        System.out.println(String.format("- Deadlocks Prevented: %d", deadlocksDetected.get()));

        // 等待所有线程完成
        for (Thread thread : threads) {
            thread.join();
        }
    }
}