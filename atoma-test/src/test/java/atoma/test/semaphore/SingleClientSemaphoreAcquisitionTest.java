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

public class SingleClientSemaphoreAcquisitionTest extends BaseTest {

  @DisplayName("TEST-SEM-ACQ: 单个客户端成功获取信号量许可，验证租约创建")
  @Test
  public void testSingleClientSemaphoreAcquisition() throws Exception {

    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Lease lease = client.grantLease(Duration.ofSeconds(3));

      int cnt = 0;
      Semaphore semaphore = lease.getSemaphore("TEST-0002", 20);
      for (int i = 0; i < 20; i++) {
        semaphore.acquire(1);

        ++cnt;
      }

      Assertions.assertThat(semaphore.drainPermits()).isEqualTo(20);
      Assertions.assertThat(semaphore.availablePermits()).isEqualTo(0);

      semaphore.release(cnt);

      semaphore.close();
      lease.revoke();

      Assertions.assertThat(cnt).isEqualTo(20);

    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
