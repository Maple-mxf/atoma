package atoma.test.cdl;

import atoma.api.synchronizer.CountDownLatch;
import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AwaitOnZeroCountDownLatchTest extends BaseTest {

  @DisplayName("DCL-TC-004: 初始化count为0时await()立即返回")
  @Test
  public void testAwaitOnZeroCount() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    CountDownLatch latch = client.getCountDownLatch("TestCountDown-004", 0);
    try {
      // await() should return immediately without blocking
      boolean result = latch.await(1, TimeUnit.MILLISECONDS);
      Assertions.assertTrue(result);
    } finally {
      latch.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
