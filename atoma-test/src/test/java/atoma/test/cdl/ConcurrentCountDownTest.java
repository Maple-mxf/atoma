package atoma.test.cdl;

import atoma.api.synchronizer.CountDownLatch;
import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ConcurrentCountDownTest extends BaseTest {

  @DisplayName("DCL-TC-007: 100个线程同时调用countDown()")
  @Test
  public void testConcurrentCountDown() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    int count = 100;
    CountDownLatch latch = client.getCountDownLatch("TestCountDown-007", count);
    ExecutorService executor = Executors.newFixedThreadPool(count);

    try {
      for (int i = 0; i < count; i++) {
        executor.submit(latch::countDown);
      }

      boolean awaitResult = latch.await(10, TimeUnit.SECONDS);
      Assertions.assertTrue(awaitResult);
      Assertions.assertEquals(0, latch.getCount());

    } finally {
      executor.shutdown();
      latch.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
