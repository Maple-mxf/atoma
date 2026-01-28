package atoma.test.rwlock;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

/**
 * 读写锁测试套件
 *
 * <p>运行所有读写锁相关的测试用例
 */
@Suite
@SuiteDisplayName("ReadWrite Lock Test Suite")
@SelectClasses({
  // 锁获取测试
  SingleClientReadLockAcquisitionTest.class,
  SingleClientWriteLockAcquisitionTest.class,
  MultiClientReadLockAcquisitionTest.class,
  WriteLockBlocksReadLockTest.class,
  ReadLockBlocksWriteLockTest.class,
  LeaseRenewalFailureAutoReleaseTest.class,
  LockAcquisitionTimeoutTest.class,
  LockAcquisitionRetryTest.class,
  ClientCrashRecoveryTest.class,
  HighConcurrencyReadWriteLockTest.class,

  // 锁释放测试
  LockReleaseTest.class
})
public class ReadWriteLockTestSuite {
  // 测试套件类，不需要实现任何方法
}
