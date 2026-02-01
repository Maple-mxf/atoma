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

public class SemaphoreFullBlockingAcquisitionTest extends BaseTest {

  @DisplayName("TEST-SEM-ACQ-003: 信号量已满时获取许可被阻塞，验证容量限制")
  @Test
  public void testSemaphoreFullBlockingAcquisition() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(3));

      // 创建一个容量为5的信号量
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-ACQ-003", 5);

      // 先获取全部5个许可，使信号量为空
      semaphore.acquire(5);

      // 验证没有可用许可
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(0);

      // 尝试再获取1个许可，在指定时间内应该超时（被阻塞）
      long startTime = System.currentTimeMillis();
      try {
        semaphore.acquire(1, 1000L, TimeUnit.MILLISECONDS); // 等待1秒
        Assertions.fail("Should have timed out");
      } catch (RuntimeException e) {
        // 预期的超时异常
        long elapsedTime = System.currentTimeMillis() - startTime;
        Assertions.assertThat(elapsedTime).isGreaterThanOrEqualTo(1000);
      }

      semaphore.close();
      lease.revoke();

    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
