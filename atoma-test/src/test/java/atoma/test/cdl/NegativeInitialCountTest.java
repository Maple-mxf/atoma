package atoma.test.cdl;

import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledExecutorService;

public class NegativeInitialCountTest extends BaseTest {

  @DisplayName("DCL-TC-010: 初始化count为负数")
  @Test
  public void testNegativeInitialCount() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);

    try {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> {
            client.getCountDownLatch("TestCountDown-010", -1);
          });
    } finally {
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
