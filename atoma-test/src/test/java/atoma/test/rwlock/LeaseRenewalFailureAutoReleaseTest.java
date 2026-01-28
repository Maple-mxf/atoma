package atoma.test.rwlock;

import atoma.api.Lease;
import atoma.api.lock.Lock;
import atoma.api.lock.ReadWriteLock;
import atoma.client.AtomaClient;
import atoma.test.BaseTest;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static atoma.storage.mongo.command.AtomaCollectionNamespace.LEASE_NAMESPACE;
import static com.mongodb.client.model.Accumulators.addToSet;
import static com.mongodb.client.model.Accumulators.first;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.lookup;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.fields;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * 测试用例: TEST-ACQ-006 描述: 续租失败后锁自动释放，验证租约过期机制
 *
 * <p>测试目标: 1. 验证当租约续期失败时，锁会被自动释放 2. 验证其他客户端能够在锁过期后获取锁 3. 验证租约过期机制的正确性
 */
public class LeaseRenewalFailureAutoReleaseTest extends BaseTest {

  @DisplayName("TEST-ACQ-006: 验证当租约续期失败时，锁会被自动释放")
  @Test
  public void testLeaseExpirationAutoReleaseReadLock() throws Exception {
    final String resourceId = "test-lease-expiration-read";

    // 创建第一个客户端并获取读锁
    AtomaClient client1 = new AtomaClient(coordinationStore);
    Lease lease1 = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock1 = lease1.getReadWriteLock(resourceId);

    Lock readLock1 = readWriteLock1.readLock();

    readLock1.lock();

    try {
      // 验证锁已被获取
      assertThat(readLock1.isClosed()).isFalse();

      // 关闭客户端以模拟租约失效
      client1.close();

      // 等待租约过期（通常需要几秒时间）
      Thread.sleep(2000);

      // 创建第二个客户端尝试获取同一个资源的锁
      AtomaClient client2 = new AtomaClient(coordinationStore);
      Lease lease2 = atomaClient.grantLease(Duration.ofSeconds(8));
      ReadWriteLock readWriteLock2 = lease2.getReadWriteLock(resourceId);
      Lock readLock2 = readWriteLock2.readLock();

      // 第二个客户端应该能够成功获取读锁
      readLock2.lock();
      try {
        assertThat(readLock2.isClosed()).isFalse();
      } finally {
        readLock2.unlock();
        client2.close();
      }

    } finally {
      // 尝试释放第一个锁（可能已经自动释放）
      try {
        readLock1.unlock();
      } catch (Exception e) {
        // 忽略，可能已经自动释放了
      }

      client1.close();
    }
  }

  @DisplayName("TEST-ACQ-006: 验证当租约续期失败时，锁会被自动释放")
  @Test
  public void testLeaseExpirationAutoReleaseWriteLock() throws Exception {
    final String resourceId = "test-lease-expiration-write";

    // 创建第一个客户端并获取写锁
    AtomaClient client1 = new AtomaClient(coordinationStore);
    Lease lease1 = client1.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock1 = lease1.getReadWriteLock(resourceId);
    Lock writeLock1 = readWriteLock1.writeLock();

    writeLock1.lock();

    try {
      // 验证锁已被获取
      assertThat(writeLock1.isClosed()).isFalse();

      // 关闭客户端以模拟租约失效
      client1.close();
      System.err.println("Client1 closed");

      // 等待租约过期
      Thread.sleep(5000);

      // 创建第二个客户端尝试获取同一个资源的写锁
      AtomaClient client2 = new AtomaClient(coordinationStore);
      Lease lease2 = client2.grantLease(Duration.ofSeconds(8));
      ReadWriteLock readWriteLock2 = lease2.getReadWriteLock(resourceId);
      Lock writeLock2 = readWriteLock2.writeLock();

      // 第二个客户端应该能够成功获取写锁
      writeLock2.lock();
      try {
        assertThat(writeLock2.isClosed()).isFalse();
      } finally {
        writeLock2.unlock();
        client2.close();
      }

    } finally {
      // 尝试释放第一个锁（可能已经自动释放）
      try {
        writeLock1.unlock();
      } catch (Exception e) {
        // 忽略，可能已经自动释放了
      }

      client1.close();
    }
  }

  @Test
  public void testGracefulShutdownWithActiveLocks() throws Exception {
    final String resourceId = "test-graceful-shutdown";

    // 创建客户端并同时获取读锁和写锁
    AtomaClient client = new AtomaClient(newMongoCoordinationStore());
    Lease lease = client.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease.getReadWriteLock(resourceId);
    Lock readLock = readWriteLock.readLock();

    // 获取读锁
    readLock.lock();

    // 创建另一个客户端尝试获取写锁（应该被阻塞）
    AtomaClient client2 = new AtomaClient(newMongoCoordinationStore());
    Lease lease2 = client2.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock2 = lease2.getReadWriteLock(resourceId);
    Lock writeLock2 = readWriteLock2.writeLock();

    AtomicBoolean writeLockAcquired = new AtomicBoolean(false);
    Thread writeThread =
        new Thread(
            () -> {
              try {
                writeLock2.lock(5, TimeUnit.SECONDS);
                writeLockAcquired.set(true);
                writeLock2.unlock();
              } catch (Exception e) {
                // 预期行为：在第一个客户端关闭前无法获取写锁
                e.printStackTrace();
              }
            });

    writeThread.start();

    // 等待写锁尝试
    Thread.sleep(2000);

    // 验证写锁尚未被获取
    assertThat(writeLockAcquired.get()).isFalse();

    // 优雅关闭第一个客户端
    client.close();
    System.err.println("Client1 closed." + System.nanoTime());

    // 等待写线程完成
    writeThread.join();

    // 验证写锁现在可以获取
    assertThat(writeLockAcquired.get()).isTrue();

    client2.close();
  }
}
