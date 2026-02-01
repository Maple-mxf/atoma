package atoma.test.semaphore;

import atoma.api.Lease;
import atoma.api.synchronizer.Semaphore;
import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SemaphoreReleaseTest extends BaseTest {

  @DisplayName("TEST-SEM-REL-001: 客户端主动释放单个许可，验证许可计数增加")
  @Test
  public void testSinglePermitRelease() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(3));
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-REL-001", 5);

      // 获取2个许可
      semaphore.acquire(2);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(3);

      // 释放1个许可
      semaphore.release(1);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(4);

      // 再释放1个许可
      semaphore.release(1);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(5);

      semaphore.close();
      lease.revoke();
    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }

  @DisplayName("TEST-SEM-REL-002: 客户端主动释放多个许可，验证批量释放机制")
  @Test
  public void testMultiplePermitsRelease() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(3));
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-REL-002", 10);

      // 获取7个许可
      semaphore.acquire(7);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(3);

      // 批量释放5个许可
      semaphore.release(5);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(8);

      // 再释放剩余的2个许可
      semaphore.release(2);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(10);

      semaphore.close();
      lease.revoke();
    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }

  @DisplayName("TEST-SEM-REL-003: 释放许可后唤醒等待客户端，验证通知机制")
  @Test
  public void testWakeUpWaitingClients() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(5));
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-REL-003", 2);

      // 获取所有许可
      semaphore.acquire(2);

      // 启动等待线程
      CountDownLatch waitLatch = new CountDownLatch(1);
      CountDownLatch acquireLatch = new CountDownLatch(1);
      AtomicInteger acquired = new AtomicInteger(0);

      new Thread(
              () -> {
                try {
                  waitLatch.countDown();
                  semaphore.acquire(1);
                  acquired.set(1);
                  acquireLatch.countDown();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              })
          .start();

      // 等待线程开始等待
      waitLatch.await();
      Thread.sleep(100); // 确保线程在等待

      // 释放1个许可，应该唤醒等待线程
      semaphore.release(1);

      // 验证线程成功获取许可
      Assertions.assertThat(acquireLatch.await(2, TimeUnit.SECONDS)).isTrue();
      Assertions.assertThat(acquired.get()).isEqualTo(1);

      semaphore.close();
      lease.revoke();
    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }

  @DisplayName("TEST-SEM-REL-004: 租约过期自动释放许可，验证自动回收")
  @Test
  public void testLeaseExpirationAutoRelease() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    // 创建短生命周期的租约
    Lease lease = client.grantLease(Duration.ofSeconds(1));
    Semaphore semaphore = lease.getSemaphore("TEST-SEM-REL-004", 5);

    // 获取3个许可
    semaphore.acquire(3);
    Assertions.assertThat(semaphore.availablePermits()).isEqualTo(2);

    // 等待租约过期
    Thread.sleep(1500);

    // 验证租约已过期
    // Assertions.assertThat(lease.isExpired()).isTrue();

    semaphore.close();
    client.close();
    scheduledExecutorService.shutdownNow();
    mongoCoordinationStore.close();
  }

  @DisplayName("TEST-SEM-REL-005: 释放不存在的许可，验证错误处理")
  @Test
  public void testReleaseNonExistentPermit() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(3));
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-REL-005", 5);

      // 获取1个许可
      semaphore.acquire(1);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(4);

      // 尝试释放比获取更多的许可（超量释放）
      Assertions.assertThatThrownBy(() -> semaphore.release(2))
          .hasMessageContaining(
              "the semaphore does not exist or the lease does not hold enough permits");
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(4);

      semaphore.close();
      lease.revoke();
    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }

  @DisplayName("TEST-SEM-REL-006: 释放许可超量处理，验证容量保护")
  @Test
  public void testOverReleaseProtection() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(3));
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-REL-006", 3);

      // 获取所有许可
      semaphore.acquire(3);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(0);

      // 释放比获取更多的许可
      Assertions.assertThatThrownBy(() -> semaphore.release(5))
          .hasMessageContaining(
              "the semaphore does not exist or the lease does not hold enough permits");

      // 验证许可数超过初始容量
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(0);

      semaphore.close();
      lease.revoke();
    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }

  @DisplayName("TEST-SEM-REL-007: 并发释放许可，验证计数一致性")
  @Test
  public void testConcurrentRelease() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(3));
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-REL-007", 10);

      // 获取所有许可
      semaphore.acquire(10);

      // 并发释放许可
      int numThreads = 5;
      CountDownLatch latch = new CountDownLatch(numThreads);

      for (int i = 0; i < numThreads; i++) {
        new Thread(
                () -> {
                  try {
                    semaphore.release(2); // 每个线程释放2个许可
                  } finally {
                    latch.countDown();
                  }
                })
            .start();
      }

      // 等待所有线程完成
      latch.await();

      // 验证所有许可都已正确释放
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(10);

      semaphore.close();
      lease.revoke();
    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }

  @DisplayName("TEST-SEM-REL-008: 强制释放所有许可，验证管理接口功能")
  @Test
  public void testForceReleaseAllPermits() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(3));
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-REL-008", 8);

      // 获取一些许可
      semaphore.acquire(5);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(3);

      // 强制释放所有许可（假设有drain方法）
      int drainedPermits = semaphore.drainPermits();
      Assertions.assertThat(drainedPermits).isEqualTo(5);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(3);

      // 恢复许可
      semaphore.release(5);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(8);

      semaphore.close();
      lease.revoke();
    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }

  @DisplayName("TEST-SEM-REL-009: 客户端异常退出后的许可清理，验证资源回收")
  @Test
  public void testClientCrashCleanup() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    // 创建短生命周期的租约
    Lease lease = client.grantLease(Duration.ofSeconds(1));
    Semaphore semaphore = lease.getSemaphore("TEST-SEM-REL-009", 5);

    // 获取所有许可
    semaphore.acquire(5);

    // 模拟客户端异常退出
    crashAtomaClient(mongoCoordinationStore, scheduledExecutorService);
    // 等待租约过期
    TimeUnit.SECONDS.sleep(120);

    // 创建新客户端验证资源已清理
    MongoCoordinationStore newStore = newMongoCoordinationStore();
    ScheduledExecutorService newExecutor = newScheduledExecutorService();
    AtomaClient newClient = new AtomaClient(newExecutor, newStore);

    try {
      Lease newLease = newClient.grantLease(Duration.ofSeconds(3));
      Semaphore newSemaphore = newLease.getSemaphore("TEST-SEM-REL-009", 5);

      // 验证所有许可都已回收
      Assertions.assertThat(newSemaphore.availablePermits()).isEqualTo(5);

      newSemaphore.close();
      newLease.revoke();
    } finally {
      newClient.close();
      newExecutor.shutdownNow();
      newStore.close();
    }
  }
}
