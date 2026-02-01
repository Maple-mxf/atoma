package atoma.test.cdl;

import atoma.api.synchronizer.CountDownLatch;
import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledExecutorService;

public class InterruptOnAwaitTest extends BaseTest {

  @DisplayName("DCL-TC-011: 在await()时中断线程")
  @Test
  public void testInterruptOnAwait() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    CountDownLatch latch = client.getCountDownLatch("TestCountDown-011", 1);

    Thread t =
        new Thread(
            () -> {
              try {
                latch.await();
                Assertions.fail("Should have thrown InterruptedException");
              } catch (InterruptedException e) {
                e.printStackTrace();
                // This is expected
                Assertions.assertTrue(Thread.currentThread().isInterrupted());
              }
            });

    try {
      t.start();
      Thread.sleep(500); // Give the thread time to start awaiting
      t.interrupt();
      t.join(1000); // Wait for the thread to finish
      Assertions.assertFalse(t.isAlive());
    } finally {
      latch.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
