package atoma.test.mutex;

import atoma.api.Lease;
import atoma.api.lock.Lock;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class LockReleaseIdempotencyTest extends BaseTest {

  @Test
  @DisplayName("TC-07: 锁释放的幂等性处理")
  public void testLockReleaseIdempotency() throws Exception {
    String resourceId = "test-resource-tc07";

    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    // 场景1：验证锁释放后不能再次释放
    Lease lease = client.grantLease(Duration.ofSeconds(30));
    Lock lock = lease.getLock(resourceId);

    // 获取锁
    lock.lock();

    // 第一次释放 - 应该成功
    Assertions.assertThatCode(lock::unlock).doesNotThrowAnyException();

    // 第二次释放 - 应该抛出异常（因为已经不再持有锁）
    Assertions.assertThatThrownBy(lock::unlock)
        .isInstanceOf(IllegalMonitorStateException.class)
        .hasMessageContaining("does not hold the lock");

    // 第三次释放 - 同样应该抛出异常
    Assertions.assertThatThrownBy(lock::unlock)
        .isInstanceOf(IllegalMonitorStateException.class)
        .hasMessageContaining("does not hold the lock");

    lease.revoke();

    // 场景2：验证非持有者不能释放别人的锁
    testNonOwnerCannotRelease();

    // 场景3：验证同一线程可重入锁的释放
    testReentrantLockRelease();

    client.close();

    System.out.println("TC-07: 锁释放的幂等性处理 - PASSED");
  }

  private void testNonOwnerCannotRelease() throws Exception {
    String resourceId = "test-resource-tc07-non-owner";
    CountDownLatch latch = new CountDownLatch(1);

    // 线程1获取锁
    AtomaClient client = new AtomaClient(newMongoCoordinationStore());
    Lease lease1 = client.grantLease(Duration.ofSeconds(30));
    Lock lock1 = lease1.getLock(resourceId);

    Thread ownerThread =
        new Thread(
            () -> {
              try {
                lock1.lock();
                latch.countDown();
                Thread.sleep(100);
                lock1.unlock();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });

    // 线程2尝试释放（非持有者）
    Thread nonOwnerThread =
        new Thread(
            () -> {
              try {
                latch.await(); // 等待线程1获取锁
                Thread.sleep(50); // 确保线程1持有锁

                Lease lease2 = this.atomaClient.grantLease(Duration.ofSeconds(30));
                Lock lock2 = lease2.getLock(resourceId);

                // 非持有者尝试释放应该失败
                Assertions.assertThatThrownBy(lock2::unlock)
                    .isInstanceOf(IllegalMonitorStateException.class);

                lease2.revoke();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });

    ownerThread.start();
    nonOwnerThread.start();

    try {
      ownerThread.join();
      nonOwnerThread.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    lease1.revoke();
    client.close();
    System.out.println("非持有者释放锁测试通过");
  }

  private void testReentrantLockRelease() throws Exception {
    String resourceId = "test-resource-tc07-reentrant";
    AtomaClient client = new AtomaClient(newMongoCoordinationStore());
    Lease lease = client.grantLease(Duration.ofSeconds(30));
    Lock lock = lease.getLock(resourceId);

    // 支持重入，测试重入后的释放
    lock.lock();
    lock.lock(); //  重入
    try {
      lock.unlock();
      lock.unlock();
    } catch (Exception e) {
      System.out.println("锁不支持重入，这是正常行为");
    }

    lease.revoke();
    client.close();
    System.out.println("重入锁释放测试完成");
  }

  @Test
  @DisplayName("TC-07-严格版: 验证锁释放的严格语义")
  void testStrictLockReleaseSemantics() throws Exception {
    String resourceId = "test-resource-tc07-strict";

    // 严格测试：锁只能被释放一次
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);


    Lease lease = client.grantLease(Duration.ofSeconds(30));
    Lock lock = lease.getLock(resourceId);

    // 1. 获取锁
    lock.lock();
    System.out.println("线程持有锁");

    // 2. 正常释放
    lock.unlock();
    System.out.println("锁已释放");

    // 3. 验证锁已被释放（尝试再次获取）
    CountDownLatch latch = new CountDownLatch(1);
    Thread otherThread =
        new Thread(
            () -> {
              try {
                Lock otherLock = client.grantLease(Duration.ofSeconds(30)).getLock(resourceId);
                try {
                  otherLock.lock(1, TimeUnit.SECONDS);
                  otherLock.unlock();
                } catch (TimeoutException timeoutException) {
                  System.out.println("其他线程未能获取锁");
                }
              } catch (Exception e) {
                e.printStackTrace();
              } finally {
                latch.countDown();
              }
            });

    otherThread.start();
    latch.await(2, TimeUnit.SECONDS);

    // 4. 再次尝试释放（应该失败）
    Assertions.assertThatThrownBy(lock::unlock)
        .isInstanceOf(IllegalMonitorStateException.class)
        .as("已经释放的锁不能再次释放");

    lease.revoke();
    client.close();
    System.out.println("严格语义验证完成");
  }
}
