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

public class ClientCrashAutoReleaseTest extends BaseTest {

  @DisplayName("TEST-SEM-ACQ-008: 客户端崩溃后许可自动释放，验证租约过期清理")
  @Test
  public void testClientCrashAutoRelease() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    // 创建一个短生命周期的租约和信号量
    Lease lease = client.grantLease(Duration.ofSeconds(1));
    Semaphore semaphore = lease.getSemaphore("TEST-SEM-ACQ-008", 5);

    // 获取3个许可
    semaphore.acquire(3);

    // 验证剩余许可数
    Assertions.assertThat(semaphore.availablePermits()).isEqualTo(2);

    // 关闭客户端模拟崩溃（不正常释放资源）
    crashAtomaClient(mongoCoordinationStore, scheduledExecutorService);

    // 等待租约过期
    TimeUnit.SECONDS.sleep(120);

    // 创建新的客户端验证许可是否被自动释放
    MongoCoordinationStore newMongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService newScheduledExecutorService = newScheduledExecutorService();
    AtomaClient newClient = new AtomaClient(newScheduledExecutorService, newMongoCoordinationStore);

    try {
      Lease newLease = newClient.grantLease(Duration.ofSeconds(3));
      Semaphore newSemaphore = newLease.getSemaphore("TEST-SEM-ACQ-008", 5);

      // 验证许可已被自动释放，应该有5个可用许可
      Assertions.assertThat(newSemaphore.availablePermits()).isEqualTo(5);

      newSemaphore.close();
      newLease.revoke();

    } finally {
      newClient.close();
      newScheduledExecutorService.shutdownNow();
      newMongoCoordinationStore.close();
    }
  }
}
