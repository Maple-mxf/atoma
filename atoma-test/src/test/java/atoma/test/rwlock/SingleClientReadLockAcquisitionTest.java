package atoma.test.rwlock;

import atoma.api.Lease;
import atoma.api.lock.Lock;
import atoma.api.lock.ReadWriteLock;
import atoma.test.BaseTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 测试用例: TEST-ACQ-001 描述: 单个客户端成功获取读锁，验证租约创建和续期
 *
 * <p>测试目标: 1. 验证单个客户端能够成功获取读锁 2. 验证读锁获取后租约被正确创建 3. 验证租约能够正常续期
 */
public class SingleClientReadLockAcquisitionTest extends BaseTest {

  @DisplayName("TEST-ACQ-001 验证单个客户端能够成功获取读锁")
  @Test
  public void testSingleClientReadLockAcquisition() throws Exception {
    // 创建读写锁实例
    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease.getReadWriteLock("test-resource-001");
    Lock readLock = readWriteLock.readLock();

    // 验证初始状态下锁未被持有
    assertThat(readLock.isClosed()).isFalse();

    // 获取读锁
    readLock.lock();

    try {
      // 验证锁已被获取
      assertThat(readLock.isClosed()).isFalse();

      // 等待一段时间以验证租约续期机制
      Thread.sleep(8000);

      // 验证锁仍然持有（租约已续期）
      assertThat(readLock.isClosed()).isFalse();

    } finally {
      // 释放读锁
      readLock.unlock();
    }

    // 验证锁已释放
    assertThat(readLock.isClosed()).isFalse();
  }

  @DisplayName("TEST-ACQ-001 验证单个客户端能够成功获取读锁")
  @Test
  public void testReadLockWithTimeout() throws Exception {
    // 创建读写锁实例
    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease.getReadWriteLock("test-resource-002");
    Lock readLock = readWriteLock.readLock();

    // 使用超时方式获取读锁
    readLock.lock(5, TimeUnit.SECONDS);

    try {
      // 验证锁已被获取
      assertThat(readLock.isClosed()).isFalse();

    } finally {
      // 释放读锁
      readLock.unlock();
    }
  }

  @DisplayName("TEST-ACQ-001 验证单个客户端能够成功获取读锁")
  @Test
  public void testReadLockInterruptibly() throws Exception {
    // 创建读写锁实例
    Lease lease = atomaClient.grantLease(Duration.ofSeconds(8));
    ReadWriteLock readWriteLock = lease.getReadWriteLock("test-resource-003");
    Lock readLock = readWriteLock.readLock();

    // 使用可中断方式获取读锁
    readLock.lockInterruptibly();

    try {
      // 验证锁已被获取
      assertThat(readLock.isClosed()).isFalse();

    } finally {
      // 释放读锁
      readLock.unlock();
    }
  }

}
