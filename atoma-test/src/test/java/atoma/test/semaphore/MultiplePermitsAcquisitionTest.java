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

public class MultiplePermitsAcquisitionTest extends BaseTest {

  @DisplayName("TEST-SEM-ACQ-002: 获取多个许可并验证计数减少，验证许可计数机制")
  @Test
  public void testMultiplePermitsAcquisition() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(3));

      // 创建一个容量为10的信号量
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-ACQ-002", 10);

      // 检查初始可用许可数
      int initialAvailable = semaphore.availablePermits();
      Assertions.assertThat(initialAvailable).isEqualTo(10);

      // 获取5个许可
      semaphore.acquire(5);

      // 验证剩余许可数
      int remainingAvailable = semaphore.availablePermits();
      Assertions.assertThat(remainingAvailable).isEqualTo(5);

      // 再获取3个许可
      semaphore.acquire(3);

      // 验证剩余许可数
      remainingAvailable = semaphore.availablePermits();
      Assertions.assertThat(remainingAvailable).isEqualTo(2);

      semaphore.close();
      lease.revoke();

    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
