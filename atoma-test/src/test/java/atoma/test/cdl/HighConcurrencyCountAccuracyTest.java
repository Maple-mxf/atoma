package atoma.test.cdl;

import atoma.api.synchronizer.CountDownLatch;
import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HighConcurrencyCountAccuracyTest extends BaseTest {

  @DisplayName("DCL-TC-009: 高并发下count的准确性验证")
  @Test
  public void testHighConcurrencyCountAccuracy() throws Exception {
    int numClients = 4;
    int threadsPerClient = 25;
    int initialCount = numClients * threadsPerClient;

    List<AtomaClient> clients = new ArrayList<>();
    List<ScheduledExecutorService> schedulers = new ArrayList<>();
    List<MongoCoordinationStore> stores = new ArrayList<>();
    List<ExecutorService> executors = new ArrayList<>();

    CountDownLatch masterLatch = null;

    try {
      for (int i = 0; i < numClients; i++) {
        MongoCoordinationStore store = newMongoCoordinationStore();
        stores.add(store);
        ScheduledExecutorService scheduler = newScheduledExecutorService();
        schedulers.add(scheduler);
        AtomaClient client = new AtomaClient(scheduler, store);
        clients.add(client);
        executors.add(Executors.newFixedThreadPool(threadsPerClient));
      }

      masterLatch = clients.get(0).getCountDownLatch("TestCountDown-009", initialCount);

      for (int i = 0; i < numClients; i++) {
        final AtomaClient client = clients.get(i);
        final CountDownLatch latch = client.getCountDownLatch("TestCountDown-009", initialCount);
        final ExecutorService executor = executors.get(i);
        for (int j = 0; j < threadsPerClient; j++) {
          executor.submit(latch::countDown);
        }
      }

      boolean awaitResult = masterLatch.await(30, TimeUnit.SECONDS);
      Assertions.assertTrue(awaitResult, "Latch should have counted down to zero");
      Assertions.assertEquals(0, masterLatch.getCount());

    } finally {
      if (masterLatch != null) {
        masterLatch.close();
      }
      for (ExecutorService executor : executors) {
        executor.shutdown();
      }
      for (AtomaClient client : clients) {
        client.close();
      }
      for (ScheduledExecutorService scheduler : schedulers) {
        scheduler.shutdownNow();
      }
      for (MongoCoordinationStore store : stores) {
        store.close();
      }
    }
  }
}
