package atoma.test.cdl;

import atoma.api.synchronizer.CountDownLatch;
import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledExecutorService;

public class ToStringIntegrityTest extends BaseTest {

    @DisplayName("DCL-TC-029: toString()方法的信息完整性")
    @Test
    public void testToStringIntegrity() throws Exception {
        MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
        ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
        AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
        String latchName = "TestCountDown-029";
        int initialCount = 3;
        CountDownLatch latch = client.getCountDownLatch(latchName, initialCount);
        try {
            String toString = latch.toString();
            Assertions.assertNotNull(toString);
            Assertions.assertTrue(toString.contains(latchName));
            Assertions.assertTrue(toString.contains("count=" + initialCount));

            latch.countDown();
            toString = latch.toString();
            Assertions.assertTrue(toString.contains("count=" + (initialCount - 1)));

        } finally {
            latch.close();
            client.close();
            scheduledExecutorService.shutdownNow();
            mongoCoordinationStore.close();
        }
    }
}
