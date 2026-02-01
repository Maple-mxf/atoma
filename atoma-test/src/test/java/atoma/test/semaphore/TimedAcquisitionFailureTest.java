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

public class TimedAcquisitionFailureTest extends BaseTest {

  @DisplayName("TEST-SEM-ACQ-004: 带超时的许可获取，超时后返回失败，验证超时控制")
  @Test
  public void testTimedAcquisitionFailure() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(3));

      // 创建一个容量为3的信号量
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-ACQ-004", 3);

      // 先获取全部3个许可，使信号量为空
      semaphore.acquire(3);

      // 验证没有可用许可
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(0);

      // 尝试再获取1个许可，设置超时时间为500毫秒
      long startTime = System.currentTimeMillis();
      boolean acquired = false;
      try {
        semaphore.acquire(1, 500L, TimeUnit.MILLISECONDS);
        acquired = true;
      } catch (RuntimeException e) {
        // 预期会抛出超时异常
        e.printStackTrace();
      }

      long elapsedTime = System.currentTimeMillis() - startTime;

      // 验证获取失败且超时时间大致正确
      Assertions.assertThat(acquired).isFalse();
      Assertions.assertThat(elapsedTime).isGreaterThanOrEqualTo(500);

      semaphore.close();
      lease.revoke();

    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
