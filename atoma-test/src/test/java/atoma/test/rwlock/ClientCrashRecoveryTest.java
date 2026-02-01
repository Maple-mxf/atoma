package atoma.test.rwlock;

import atoma.api.Lease;
import atoma.api.coordination.CoordinationStore;
import atoma.api.lock.Lock;
import atoma.api.lock.ReadWriteLock;
import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 测试用例: TEST-ACQ-009 和 TEST-REL-008 描述: 客户端崩溃后锁自动清理和异常退出后的锁释放
 *
 * <p>测试目标: 1. 验证客户端崩溃后锁能够被自动清理 2. 验证其他客户端能够在锁清理后获取锁 3. 验证异常退出的处理机制
 */
public class ClientCrashRecoveryTest extends BaseTest {

  @DisplayName("TEST-ACQ-009: 验证客户端崩溃后锁能够被自动清理")
  @Test
  public void testClientCrashAutoCleanup() throws Exception {
    final String resourceId = "test-crash-cleanup-resource";

    // 创建第一个客户端并获取写锁
    AtomaClient client1 = new AtomaClient(newMongoCoordinationStore());
    Lease lease1 = client1.grantLease(Duration.ofSeconds(8));

    ReadWriteLock readWriteLock1 = lease1.getReadWriteLock(resourceId);
    Lock writeLock1 = readWriteLock1.writeLock();

    writeLock1.lock();

    // 验证锁被获取
    assertThat(writeLock1.isClosed()).isFalse();

    // 等待系统检测到客户端失效并清理锁
    lease1.revoke();
    Thread.sleep(3000);

    // 创建第二个客户端尝试获取同一个资源的锁
    AtomaClient client2 = new AtomaClient(newMongoCoordinationStore());
    Lease lease2 = client2.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock2 = lease2.getReadWriteLock(resourceId);
    Lock writeLock2 = readWriteLock2.writeLock();

    // 第二个客户端应该能够成功获取锁
    writeLock2.lock();
    try {
      assertThat(writeLock2.isClosed()).isFalse();
    } finally {
      writeLock2.unlock();
      lease1.revoke();
      client2.close();
    }
  }

  @DisplayName("TEST-ACQ-009: 验证客户端崩溃后锁能够被自动清理")
  @Test
  public void testAbnormalClientExit() throws Exception {
    final String resourceId = "test-abnormal-exit-resource";

    AtomicBoolean lockAcquired = new AtomicBoolean(false);
    CountDownLatch abnormalExitCompleted = new CountDownLatch(1);

    // 创建线程模拟异常退出的客户端
    Thread abnormalClient =
        new Thread(
            () -> {
              ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
              CoordinationStore coordinationStore1 = newMongoCoordinationStore();
              try {
                AtomaClient atomaClient1 =
                    new AtomaClient(scheduledExecutorService, coordinationStore1);

                Lease lease1 = atomaClient1.grantLease(Duration.ofSeconds(1));

                ReadWriteLock readWriteLock = lease1.getReadWriteLock(resourceId);
                Lock readLock = readWriteLock.readLock();

                // 获取读锁
                readLock.lock();

                // 模拟异常：抛出异常而不正常释放锁
                throw new RuntimeException("Simulated abnormal exit");

              } catch (Exception e) {
                e.printStackTrace();
                try {
                  crashAtomaClient(coordinationStore1, scheduledExecutorService);
                } catch (Exception e2) {
                  e2.printStackTrace();
                }
                abnormalExitCompleted.countDown();
              }
            });

    abnormalClient.start();
    abnormalExitCompleted.await();
    abnormalClient.join();

    // 等待系统处理异常退出
    Thread.sleep(3000);

    // 创建新客户端验证锁已被释放
    System.err.println("创建新客户端验证锁已被释放");
    AtomaClient newClient = new AtomaClient(coordinationStore);
    Lease lease1 = newClient.grantLease(Duration.ofSeconds(2));
    ReadWriteLock readWriteLock = lease1.getReadWriteLock(resourceId);
    Lock writeLock = readWriteLock.writeLock();

    // 应该能够获取写锁
    writeLock.lock();
    try {
      lockAcquired.set(true);
    } finally {
      writeLock.unlock();
      newClient.close();
    }

    assertThat(lockAcquired.get()).isTrue();
  }

  @DisplayName("TEST-ACQ-009: 验证多个客户端崩溃后锁能够被自动清理")
  @Test
  public void testMultipleClientCrash() throws Exception {
    final int clientCount = 5;
    final String resourceId = "test-multi-crash-resource";

    CountDownLatch allCrashed = new CountDownLatch(clientCount);

    for (int i = 0; i < clientCount; i++) {
      final int clientId = i;
      // 创建线程模拟崩溃
      Thread crashThread =
          new Thread(
              () -> {
                try {
                  ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
                  MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
                  AtomaClient client =
                      new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
                  Lease lease1 = client.grantLease(Duration.ofSeconds(1));
                  ReadWriteLock readWriteLock = lease1.getReadWriteLock(resourceId);
                  Lock readLock = readWriteLock.readLock();
                  readLock.lock();
                  Thread.sleep(100 * clientId); // 错开崩溃时间
                  crashAtomaClient(mongoCoordinationStore, scheduledExecutorService);
                  allCrashed.countDown();
                } catch (Exception e) {
                  e.printStackTrace();
                }
              });
      crashThread.start();
    }

    // 等待所有客户端崩溃
    allCrashed.await();

    // 等待系统清理
    Thread.sleep(3000);

    // 创建新客户端尝试获取写锁
    AtomaClient newClient = new AtomaClient(newMongoCoordinationStore());
    System.err.println("创建新客户端尝试获取写锁");
    Lease lease1 = newClient.grantLease(Duration.ofSeconds(3));
    ReadWriteLock readWriteLock = lease1.getReadWriteLock(resourceId);
    Lock writeLock = readWriteLock.writeLock();

    // 应该能够获取写锁（所有读锁都已清理）
    writeLock.lock();
    try {
      assertThat(writeLock.isClosed()).isFalse();
    } finally {
      writeLock.unlock();
      newClient.close();
    }
  }

  @DisplayName("TEST-ACQ-009: 验证多个客户端崩溃后锁能够被自动清理")
  @Test
  public void testClientCrashDuringLockAcquisition() throws Exception {
    final String resourceId = "test-crash-during-acquisition-resource";

    // 首先获取写锁
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();

    AtomaClient atomaClient1 = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    Lease lease1 = atomaClient1.grantLease(Duration.ofSeconds(1));
    ReadWriteLock readWriteLock1 = lease1.getReadWriteLock(resourceId);
    Lock writeLock1 = readWriteLock1.writeLock();
    writeLock1.lock();

    ReadWriteLock readWriteLock = lease1.getReadWriteLock(resourceId);
    Lock readLock = readWriteLock.readLock();

    CountDownLatch acquisitionStarted = new CountDownLatch(1);
    CountDownLatch crashSimulated = new CountDownLatch(1);

    // 创建尝试获取锁的客户端
    Thread acquisitionThread =
        new Thread(
            () -> {
              try {

                acquisitionStarted.countDown();

                // 尝试获取读锁（会被阻塞）
                readLock.lock(5, TimeUnit.SECONDS);

                // 如果获取成功，模拟崩溃
                crashSimulated.await();

                crashAtomaClient(mongoCoordinationStore, scheduledExecutorService);

              } catch (Exception e) {
                // 预期行为
              }
            });

    acquisitionThread.start();
    acquisitionStarted.await();

    // 等待一段时间让锁请求发出
    Thread.sleep(1000);

    // 等待获取线程获取锁
    Thread.sleep(1000);

    // 模拟崩溃
    crashSimulated.countDown();
    acquisitionThread.join();

    // 等待系统清理
    Thread.sleep(2000);

    // 验证可以重新获取锁
    Lock writeLock2 = readWriteLock1.writeLock();
    writeLock2.lock();
    try {
      assertThat(writeLock2.isClosed()).isFalse();
    } finally {
      writeLock2.unlock();
    }
  }
}
