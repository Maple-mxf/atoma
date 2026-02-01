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

public class RepeatedCountDownTest extends BaseTest {

  @DisplayName("DCL-TC-003 & DCL-TC-022: 重复调用countDown()方法 & 调用次数超过初始值")
  @Test
  public void testRepeatedAndExceedingCountDown() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    CountDownLatch latch = client.getCountDownLatch("TestCountDown-003-022", 2);
    try {
      Assertions.assertEquals(2, latch.getCount());

      latch.countDown();
      Assertions.assertEquals(1, latch.getCount());

      latch.countDown();
      Assertions.assertEquals(0, latch.getCount());

      // Count is now 0. Further calls should not change it (no negative count).
      latch.countDown();
      Assertions.assertEquals(0, latch.getCount());

      latch.countDown();
      Assertions.assertEquals(0, latch.getCount());

      // Await should succeed immediately
      Assertions.assertTrue(latch.await(1, TimeUnit.MILLISECONDS));

    } finally {
      latch.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
