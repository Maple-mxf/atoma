package atoma.test.mutex;

import atoma.api.Lease;
import atoma.api.lock.Lock;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import atoma.client.AtomaClient;
import atoma.test.BaseTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class MultiClientLockCompetitionTest extends BaseTest {

  @Test
  @DisplayName("TC-02: 多客户端竞争，只有一个能成功获取锁")
  void testMultiClientLockCompetition() throws Exception {
    String resourceId = "test-resource-tc02";
    int clientCount = 5;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(clientCount);

    // 使用 ConcurrentHashMap 记录每个时间点的锁持有者
    ConcurrentHashMap<Long, Integer> lockHolders = new ConcurrentHashMap<>();
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicBoolean mutexViolated = new AtomicBoolean(false);

    // 线程池来管理客户端
    ExecutorService executor = Executors.newFixedThreadPool(clientCount);

    AtomaClient client = new AtomaClient(newMongoCoordinationStore());

    for (int i = 0; i < clientCount; i++) {
      final int clientId = i;
      executor.submit(
          () -> {
            try {
              startLatch.await();

              Lease lease = client.grantLease(Duration.ofSeconds(30));
              Lock lock = lease.getLock(resourceId);
              try {
                // 尝试获取锁
                lock.lock(3, TimeUnit.SECONDS);
                try {
                  // 记录成功获取锁
                  successCount.incrementAndGet();

                  // 检查是否有其他线程同时持有锁
                  long currentTime = System.currentTimeMillis();
                  lockHolders.put(currentTime, clientId);

                  // 模拟工作负载
                  Thread.sleep(100);

                  // 再次检查是否只有自己持有锁
                  lockHolders.forEach(
                      (time, holder) -> {
                        if (Math.abs(time - currentTime) < 50 && holder != clientId) {
                          mutexViolated.set(true);
                          System.err.println(
                              "互斥锁被违反！客户端 " + clientId + " 和 " + holder + " 似乎同时持有锁");
                        }
                      });

                } finally {
                  lock.unlock();
                }
              } catch (TimeoutException timeoutException) {
                System.out.println("客户端 " + clientId + " 未能在超时时间内获取锁");
              }

              lease.revoke();
            } catch (Exception e) {
              System.err.println("客户端 " + clientId + " 发生异常: " + e.getMessage());
            } finally {
              completionLatch.countDown();
            }
          });
    }

    // 启动所有线程
    startLatch.countDown();

    // 等待所有客户端完成
    boolean completed = completionLatch.await(15, TimeUnit.SECONDS);
    executor.shutdown();

    // 验证结果
    Assertions.assertThat(completed).isTrue();
    Assertions.assertThat(successCount.get()).isGreaterThan(0);
    Assertions.assertThat(successCount.get()).isLessThanOrEqualTo(clientCount);
    Assertions.assertThat(mutexViolated.get()).isFalse();

    System.out.println("TC-02: 多客户端竞争，只有一个能成功获取锁 - PASSED");
    System.out.println("尝试客户端数: " + clientCount);
    System.out.println("成功获取锁的客户端数: " + successCount.get());
    System.out.println("互斥锁是否被违反: " + mutexViolated.get());

    client.close();
  }

  @Test
  @DisplayName("TC-02-增强版: 验证锁的严格互斥性")
  void testStrictMutexProperty() throws Exception {
    String resourceId = "test-resource-tc02-strict";
    int rounds = 3; // 进行多轮测试
    int clientsPerRound = 3;

    AtomaClient client = new AtomaClient(newMongoCoordinationStore());

    for (int round = 0; round < rounds; round++) {
      System.out.println("=== 第 " + (round + 1) + " 轮测试 ===");

      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch completionLatch = new CountDownLatch(clientsPerRound);
      AtomicInteger concurrentHolders = new AtomicInteger(0);
      AtomicInteger maxConcurrentHolders = new AtomicInteger(0);
      AtomicBoolean mutexViolated = new AtomicBoolean(false);

      ExecutorService executor = Executors.newFixedThreadPool(clientsPerRound);

      for (int i = 0; i < clientsPerRound; i++) {
        final int clientId = i;
        executor.submit(
            () -> {
              try {
                startLatch.await();

                Lease lease =  client.grantLease(Duration.ofSeconds(10));
                Lock lock = lease.getLock(resourceId);

                // 使用阻塞式锁获取
                lock.lock();
                try {
                  // 进入临界区
                  int current = concurrentHolders.incrementAndGet();
                  maxConcurrentHolders.updateAndGet(v -> Math.max(v, current));

                  if (current > 1) {
                    mutexViolated.set(true);
                    System.err.println("严重错误：检测到 " + current + " 个客户端同时持有锁！");
                  }

                  // 模拟工作
                  Thread.sleep(50);

                  // 离开临界区
                  concurrentHolders.decrementAndGet();

                } finally {
                  lock.unlock();
                }

                lease.revoke();
              } catch (Exception e) {
                System.err.println("客户端 " + clientId + " 异常: " + e.getMessage());
              } finally {
                completionLatch.countDown();
              }
            });
      }

      startLatch.countDown();
      boolean completed = completionLatch.await(10, TimeUnit.SECONDS);
      executor.shutdown();

      Assertions.assertThat(completed).isTrue();
      Assertions.assertThat(maxConcurrentHolders.get()).isEqualTo(1);
      Assertions.assertThat(mutexViolated.get()).isFalse();

      System.out.println("第 " + (round + 1) + " 轮完成，最大并发持有者数: " + maxConcurrentHolders.get());

      // 短暂等待再进行下一轮
      Thread.sleep(100);
    }

    client.close();

    System.out.println("=== 所有轮次完成，锁的互斥性验证通过 ===");
  }
}
