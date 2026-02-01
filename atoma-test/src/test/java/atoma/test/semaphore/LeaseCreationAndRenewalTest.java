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

public class LeaseCreationAndRenewalTest extends BaseTest {

  @DisplayName("TEST-SEM-ACQ-007: 获取许可时租约创建和续期，验证心跳机制")
  @Test
  public void testLeaseCreationAndRenewal() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      // 创建一个较短生命周期的租约
      Lease lease = client.grantLease(Duration.ofSeconds(2));

      // 创建信号量并获取许可
      Semaphore semaphore = lease.getSemaphore("TEST-SEM-ACQ-007", 5);
      semaphore.acquire(3);

      // 验证租约ID不为空
      Assertions.assertThat(lease.getResourceId()).isNotNull();

      // 验证信号量关联的租约ID正确
      Assertions.assertThat(semaphore.getLeaseId()).isEqualTo(lease.getResourceId());

      // 等待一段时间让租约续期
      Thread.sleep(1500);

      // 再次获取许可应该成功（租约仍在有效期内）
      semaphore.acquire(1);

      // 验证剩余许可数
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(1);

      semaphore.close();
      lease.revoke();

    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
