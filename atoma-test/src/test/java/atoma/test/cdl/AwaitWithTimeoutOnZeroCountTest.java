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

public class AwaitWithTimeoutOnZeroCountTest extends BaseTest {

  @DisplayName("DCL-TC-006: 带超时的await()在count为0时立即返回")
  @Test
  public void testAwaitWithTimeoutOnZeroCount() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    CountDownLatch latch = client.getCountDownLatch("TestCountDown-006", 0);
    try {
      // await() should return true immediately
      boolean result = latch.await(1, TimeUnit.SECONDS);
      Assertions.assertTrue(result);
    } finally {
      latch.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
