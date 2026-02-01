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
import java.util.concurrent.ScheduledExecutorService;

@Deprecated
public class SemaphoreCapacityTest extends BaseTest {

//  @DisplayName("TEST-SEM-CAP-001: 创建不同容量的信号量，验证初始化配置")
//  @Test
  public void testCreateSemaphoreWithDifferentCapacities() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(3));

      // 测试不同容量的信号量
      int[] capacities = {1, 5, 10, 100, 1000};

      for (int capacity : capacities) {
        String semaphoreName = "TEST-SEM-CAP-001-" + capacity;
        Semaphore semaphore = lease.getSemaphore(semaphoreName, capacity);

        // 验证初始可用许可数等于容量
        Assertions.assertThat(semaphore.availablePermits()).isEqualTo(capacity);

        semaphore.close();
      }

      lease.revoke();
    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }

  @DisplayName("TEST-SEM-CAP-002: 获取全部许可后信号量为空，验证容量边界")
  @Test
  public void testAcquireAllPermits() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(3));
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-CAP-002", 10);

      // 初始状态
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(10);

      // 获取全部许可
      semaphore.acquire(10);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(0);

      // 尝试获取1个许可应该失败
      boolean acquired = false;
      try {
        semaphore.acquire(1, 100L, java.util.concurrent.TimeUnit.MILLISECONDS);
        acquired = true;
      } catch (RuntimeException e) {
        // 预期失败
      }
      Assertions.assertThat(acquired).isFalse();

      semaphore.close();
      lease.revoke();
    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }

  @DisplayName("TEST-SEM-CAP-003: 容量为零的信号量行为，验证特殊情况处理")
  @Test
  public void testZeroCapacitySemaphore() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(3));
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-CAP-003", 0);

      // 验证初始可用许可数为0
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(0);

      // 尝试获取许可应该失败
      boolean acquired = false;
      try {
        semaphore.acquire(1, 200L, java.util.concurrent.TimeUnit.MILLISECONDS);
        acquired = true;
      } catch (RuntimeException e) {
        // 预期失败
      }
      Assertions.assertThat(acquired).isFalse();

      semaphore.close();
      lease.revoke();
    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }

  @DisplayName("TEST-SEM-CAP-004: 动态增加信号量容量，验证扩容机制")
  @Test
  public void testDynamicCapacityIncrease() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(3));
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-CAP-004", 5);

      // 初始状态
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(5);

      // 获取所有许可
      semaphore.acquire(5);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(0);

      // 通过释放更多许可来模拟扩容（增加可用许可）
      semaphore.release(3); // 释放比获取更多的许可
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(3);

      // 再释放一些许可
      semaphore.release(2);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(5);

      semaphore.close();
      lease.revoke();
    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }

  @DisplayName("TEST-SEM-CAP-005: 动态减少信号量容量，验证缩容机制")
  @Test
  public void testDynamicCapacityDecrease() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(3));
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-CAP-005", 10);

      // 初始状态
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(10);

      // 获取一些许可
      semaphore.acquire(4);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(6);

      // 通过获取更多许可来模拟缩容
      semaphore.acquire(6);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(0);

      // 验证不能再获取更多许可
      boolean acquired = false;
      try {
        semaphore.acquire(1, 100L, java.util.concurrent.TimeUnit.MILLISECONDS);
        acquired = true;
      } catch (RuntimeException e) {
        // 预期失败
      }
      Assertions.assertThat(acquired).isFalse();

      semaphore.close();
      lease.revoke();
    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
