package atoma.test.barrier;

import atoma.api.Lease;
import atoma.api.synchronizer.CyclicBarrier;
import atoma.core.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Test case for BARRIER-TC-015: Barrier resets after all nodes arrive. */
public class BarrierTc015Test extends BaseTest {

  @DisplayName("BARRIER-TC-015: 所有节点到达后屏障重置")
  @Test
  public void testBarrierResetsAndIsReusable() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();
    final String barrierId = "BARRIER-TC-015";
    final int parties = 2;
    final CyclicBarrier barrier = lease.getCyclicBarrier(barrierId, parties);

    try {
      // First run
      final CountDownLatch firstPassLatch = new CountDownLatch(parties);
      System.out.println("--- First Pass ---");
      for (int i = 0; i < parties; i++) {
        new Thread(
                () -> {
                  try {
                    barrier.await(5, TimeUnit.SECONDS);
                    firstPassLatch.countDown();
                  } catch (Exception e) {
                    Assertions.fail("Await should not fail on first pass", e);
                  }
                })
            .start();
      }
      Assertions.assertTrue(
          firstPassLatch.await(6, TimeUnit.SECONDS), "All parties should pass the first barrier");
      TimeUnit.MILLISECONDS.sleep(200); // Allow time for state to settle
      Assertions.assertEquals(
          0, barrier.getNumberWaiting(), "Waiting count should be 0 after first pass");

      // Second run
      final CountDownLatch secondPassLatch = new CountDownLatch(parties);
      System.out.println("--- Second Pass ---");
      for (int i = 0; i < parties; i++) {
        new Thread(
                () -> {
                  try {
                    barrier.await(5, TimeUnit.SECONDS);
                    secondPassLatch.countDown();
                  } catch (Exception e) {
                    Assertions.fail("Await should not fail on second pass", e);
                  }
                })
            .start();
      }
      Assertions.assertTrue(
          secondPassLatch.await(6, TimeUnit.SECONDS),
          "All parties should pass the second barrier, demonstrating reusability");
      Assertions.assertEquals(
          0, barrier.getNumberWaiting(), "Waiting count should be 0 after second pass");

    } finally {
      barrier.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
