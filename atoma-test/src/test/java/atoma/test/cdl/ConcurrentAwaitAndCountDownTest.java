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

public class ConcurrentAwaitAndCountDownTest extends BaseTest {

    @DisplayName("DCL-TC-008: 多个线程同时调用await()和countDown()")
    @Test
    public void testConcurrentAwaitAndCountDown() throws Exception {
        MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
        ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
        AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
        int numThreads = 10;
        int initialCount = numThreads / 2;
        CountDownLatch latch = client.getCountDownLatch("TestCountDown-008", initialCount);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        AtomicInteger awaitSuccessCount = new AtomicInteger(0);

        try {
            // Half threads await
            for (int i = 0; i < initialCount; i++) {
                executor.submit(() -> {
                    try {
                        if (latch.await(10, TimeUnit.SECONDS)) {
                            awaitSuccessCount.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            // Give awaiters time to start
            Thread.sleep(500);

            // Other half threads countDown
            for (int i = 0; i < initialCount; i++) {
                executor.submit(latch::countDown);
            }

            executor.shutdown();
            executor.awaitTermination(15, TimeUnit.SECONDS);

            Assertions.assertEquals(initialCount, awaitSuccessCount.get());
            Assertions.assertEquals(0, latch.getCount());

        } finally {
            latch.close();
            client.close();
            scheduledExecutorService.shutdownNow();
            mongoCoordinationStore.close();
        }
    }
}
