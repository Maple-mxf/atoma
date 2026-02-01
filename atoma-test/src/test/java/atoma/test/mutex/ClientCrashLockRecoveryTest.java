//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package atoma.test.mutex;

import atoma.api.Lease;
import atoma.api.lock.Lock;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import atoma.client.AtomaClient;
import atoma.client.DefaultLease;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.test.BaseTest;
import org.assertj.core.api.Assertions;
import org.bson.BsonNull;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static atoma.storage.mongo.command.AtomaCollectionNamespace.LEASE_NAMESPACE;
import static com.mongodb.client.model.Aggregates.lookup;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static java.util.Collections.singletonList;

public class ClientCrashLockRecoveryTest extends BaseTest {
  public ClientCrashLockRecoveryTest() {}

  @Test
  @DisplayName("TC-18: 持有锁的客户端进程崩溃")
  void testClientCrashLockRecovery() throws InterruptedException {
    String resourceId = "test-resource-tc18";
    CountDownLatch lockAcquired = new CountDownLatch(1);
    CountDownLatch recoveryVerified = new CountDownLatch(1);

    Thread crashingClient =
        new Thread(
            () -> {
              try {
                ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
                MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
                AtomaClient client =
                    new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
                Lease lease1 = client.grantLease(Duration.ofSeconds(1));
                Lock lock1 = lease1.getLock(resourceId);
                lock1.lock();
                crashAtomaClient(mongoCoordinationStore, scheduledExecutorService);
                System.out.printf(
                    "Thread1 %s %s crash%n",
                    lease1.getResourceId(), Thread.currentThread().getName());
                lockAcquired.countDown();
              } catch (InterruptedException var3) {
                Thread.currentThread().interrupt();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    Thread recoveryClient =
        new Thread(
            () -> {
              try {
                lockAcquired.await();
                Thread.sleep(1500L);
                System.err.printf("Thread2 ready to lock: %s%n", Thread.currentThread().getName());
                MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
                AtomaClient client = new AtomaClient(mongoCoordinationStore);
                Lease lease2 = client.grantLease(Duration.ofSeconds(3L));
                Lock lock2 = lease2.getLock(resourceId);
                lock2.lock();
                System.err.println("Thread2  lock success.");
                recoveryVerified.countDown();
                lock2.unlock();

                lease2.revoke();
                client.close();
                mongoCoordinationStore.close();
              } catch (InterruptedException var6) {
                Thread.currentThread().interrupt();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    crashingClient.start();
    recoveryClient.start();
    Assertions.assertThat(lockAcquired.await(30L, TimeUnit.SECONDS)).isTrue();
    Assertions.assertThat(recoveryVerified.await(120L, TimeUnit.SECONDS)).isTrue();
    crashingClient.join();
    recoveryClient.join();
    System.out.println("TC-18: 持有锁的客户端进程崩溃 - PASSED");
  }
}
