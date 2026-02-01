package atoma.test.cdl;

import atoma.api.synchronizer.CountDownLatch;
import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledExecutorService;

public class GetCountAccuracyTest extends BaseTest {

    @DisplayName("DCL-TC-028: getCount()方法的准确性验证")
    @Test
    public void testGetCountAccuracy() throws Exception {
        MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
        ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
        AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
        int initialCount = 5;
        CountDownLatch latch = client.getCountDownLatch("TestCountDown-028", initialCount);
        try {
            Assertions.assertEquals(initialCount, latch.getCount());

            for (int i = initialCount; i > 0; i--) {
                latch.countDown();
                Assertions.assertEquals(i - 1, latch.getCount());
            }

            // Extra countDown calls
            latch.countDown();
            Assertions.assertEquals(0, latch.getCount());

        } finally {
            latch.close();
            client.close();
            scheduledExecutorService.shutdownNow();
            mongoCoordinationStore.close();
        }
    }
}
