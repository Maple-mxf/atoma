//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package atoma.test.mutex;

import atoma.api.Lease;
import atoma.api.Result;
import atoma.api.lock.Lock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import atoma.client.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;
import atoma.storage.mongo.command.CommandExecutor;
import atoma.test.BaseTest;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.assertj.core.api.Assertions;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ClientRestartLockRecoveryTest extends BaseTest {



  @Test
  @DisplayName("TC-21: 客户端重启后尝试恢复锁")
  void testClientRestartLockRecovery() throws Exception {
    String resourceId = "test-resource-tc21";
    CountDownLatch lockAcquired = new CountDownLatch(1);
    CountDownLatch clientRestarted = new CountDownLatch(1);
    CountDownLatch lockRecovered = new CountDownLatch(1);

    System.out.printf("start execute TC-21 %n");

    ScheduledExecutorService scheduledExecutorService = newScheduledExecutorService();
    System.out.printf("start execute TC-21 22222  %n");
    MongoCoordinationStore mongoCoordinationStore = newMongoCoordinationStore();
    System.out.printf("start execute TC-21 44444  %n");
    AtomaClient client = new AtomaClient(scheduledExecutorService, mongoCoordinationStore);
    System.out.printf("start execute TC-21 5555  %n");
    Lease lease1 = client.grantLease(Duration.ofSeconds(30L));

    System.out.printf("start execute TC-21 6666  %n");

    Lock lock1 = lease1.getLock(resourceId);

    System.out.printf("start execute TC-21 33333  %n");
    Thread originalClient =
        new Thread(
            () -> {
              try {
                lock1.lock();
                lockAcquired.countDown();
                Thread.sleep(1000L);
                lock1.unlock();
              } catch (InterruptedException var3) {
                Thread.currentThread().interrupt();
              }
            });
    Thread restartClient =
        new Thread(
            () -> {
              try {
                lockAcquired.await();
                Thread.sleep(1500L);
                clientRestarted.countDown();
                Lease lease2 = client.grantLease(Duration.ofSeconds(30L));
                Lock lock2 = lease2.getLock(resourceId);
                lock2.lock();
                lockRecovered.countDown();
                lock2.unlock();
                lease2.revoke();
              } catch (InterruptedException var7) {
                Thread.currentThread().interrupt();
              }
            });
    originalClient.start();
    restartClient.start();
    Assertions.assertThat(lockAcquired.await(2L, TimeUnit.SECONDS)).isTrue();
    Assertions.assertThat(clientRestarted.await(3L, TimeUnit.SECONDS)).isTrue();
    Assertions.assertThat(lockRecovered.await(3L, TimeUnit.SECONDS)).isTrue();
    originalClient.join();
    restartClient.join();
    lease1.revoke();
    client.close();
    mongoCoordinationStore.close();
    System.out.println("TC-21: 客户端重启后尝试恢复锁 - PASSED");
  }
}
