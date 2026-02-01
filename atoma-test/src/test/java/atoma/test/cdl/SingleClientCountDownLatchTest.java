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

public class SingleClientCountDownLatchTest extends BaseTest {

  @DisplayName("DCL-TC-001: 单线程正常等待count减为0")
  @Test
  public void testSingleClientCountDownLatchCount() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    CountDownLatch countDownLatch = client.getCountDownLatch("TestCountDown-01", 10);
    try {
      for (int i = 0; i < 10; i++) {
        countDownLatch.countDown();
      }
      Assertions.assertEquals(0, countDownLatch.getCount());
      Assertions.assertTrue(countDownLatch.await(1, TimeUnit.MILLISECONDS));

    } finally {
      countDownLatch.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
