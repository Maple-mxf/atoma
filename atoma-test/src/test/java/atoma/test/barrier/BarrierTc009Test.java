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

/**
 * Test case for BARRIER-TC-009: Node repeatedly registers to the same barrier. This is interpreted
 * as multiple threads from the same client joining the barrier.
 */
public class BarrierTc009Test extends BaseTest {

  @DisplayName("BARRIER-TC-009: 节点重复注册到同一屏障")
  @Test
  public void testRepeatedRegistration() throws Exception {
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    Lease lease = client.grantLease();

    final String barrierId = "BARRIER-TC-009";
    final int parties = 2;
    final CyclicBarrier barrier = lease.getCyclicBarrier(barrierId, parties);
    final CountDownLatch passLatch = new CountDownLatch(parties);

    try {
      for (int i = 0; i < parties; i++) {
        new Thread(
                () -> {
                  try {
                    barrier.await(5, TimeUnit.SECONDS);
                    passLatch.countDown();
                  } catch (Exception e) {
                    Assertions.fail("Await should not fail", e);
                  }
                })
            .start();
      }

      Assertions.assertTrue(
          passLatch.await(5, TimeUnit.SECONDS), "All parties should pass the barrier");
      Assertions.assertEquals(
          0,
          barrier.getNumberWaiting(),
          "Number of waiting parties should be zero after barrier is passed");

    } finally {
      barrier.close();
      client.close();
      scheduledExecutorService.shutdownNow();
      mongoCoordinationStore.close();
    }
  }
}
