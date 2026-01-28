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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import atoma.client.DefaultLease;
import atoma.test.BaseTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
                Lease lease1 = this.atomaClient.grantLease(Duration.ofSeconds(1L));
                Lock lock1 = lease1.getLock(resourceId);
                lock1.lock();
                lockAcquired.countDown();
                System.out.printf("Lease1 %s crash%n", lease1.getResourceId());

                Method cancelTimeToLive = DefaultLease.class.getDeclaredMethod(
                        "cancelTimeToLive"
                );
                cancelTimeToLive.setAccessible(true);
                cancelTimeToLive.invoke(lease1);

                Thread.sleep(1000L);
              } catch (InterruptedException var3) {
                Thread.currentThread().interrupt();
              } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new RuntimeException(e);
              }
            });
    Thread recoveryClient =
        new Thread(
            () -> {
              try {
                lockAcquired.await();
                Thread.sleep(3000L);
                Lease lease2 = this.atomaClient.grantLease(Duration.ofSeconds(30L));
                Lock lock2 = lease2.getLock(resourceId);
                lock2.lock();
                recoveryVerified.countDown();
                lock2.unlock();
                lease2.revoke();
              } catch (InterruptedException var6) {
                Thread.currentThread().interrupt();
              }
            });
    crashingClient.start();
    recoveryClient.start();
    Assertions.assertThat(lockAcquired.await(2L, TimeUnit.SECONDS)).isTrue();
    Assertions.assertThat(recoveryVerified.await(60L, TimeUnit.SECONDS)).isTrue();
    crashingClient.join();
    recoveryClient.join();
    System.out.println("TC-18: 持有锁的客户端进程崩溃 - PASSED");
  }
}
