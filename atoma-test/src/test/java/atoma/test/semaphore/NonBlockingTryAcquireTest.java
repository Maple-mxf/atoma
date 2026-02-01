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
import java.util.concurrent.TimeUnit;

public class NonBlockingTryAcquireTest extends BaseTest {


  @DisplayName("TEST-SEM-ACQ-005: 非阻塞方式尝试获取许可，立即返回结果，验证tryAcquire机制")
  @Test
  public void testNonBlockingTryAcquire() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(3));

      // 创建一个容量为2的信号量
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-ACQ-005", 2);

      // 初始状态下应该可以获取许可
      semaphore.acquire(1);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(1);

      // 再次获取许可应该成功
      semaphore.acquire(1);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(0);

      // 尝试再获取1个许可，应该立即失败（非阻塞）
      long startTime = System.currentTimeMillis();
      try {
        semaphore.acquire(1, 0L, TimeUnit.MILLISECONDS); // 立即返回
        Assertions.fail("Should have failed immediately");
      } catch (RuntimeException e) {
        // 预期的失败
        e.printStackTrace();
      }
      long endTime = System.currentTimeMillis();

      // 验证是立即返回而不是等待
      Assertions.assertThat(endTime - startTime).isLessThan(100);

      semaphore.close();
      lease.revoke();

    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
