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
import java.util.concurrent.atomic.AtomicInteger;

public class MultipleThreadsAwaitOnSameLatchTest extends BaseTest {

  @DisplayName("DCL-TC-002: 多个线程同时等待同一个latch")
  @Test
  public void testMultipleThreadsAwaitOnSameLatch() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    CountDownLatch latch = client.getCountDownLatch("TestCountDown-002", 1);

    int numThreads = 5;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    AtomicInteger awaitSuccessCount = new AtomicInteger(0);

    try {
      for (int i = 0; i < numThreads; i++) {
        executor.submit(
            () -> {
              try {
                if (latch.await(5, TimeUnit.SECONDS)) {
                  awaitSuccessCount.incrementAndGet();
                }
              } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
              }
            });
      }

      // Give threads time to start and await
      Thread.sleep(1000);

      latch.countDown();

      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);

      Assertions.assertEquals(numThreads, awaitSuccessCount.get());

    } finally {
      latch.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
