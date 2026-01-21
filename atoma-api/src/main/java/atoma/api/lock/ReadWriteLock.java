package atoma.api.lock;

import atoma.api.Leasable;

public abstract class ReadWriteLock extends Leasable {

  /**
   * 返回用于读操作的锁。
   *
   * @return 读锁实例。
   */
  public abstract Lock readLock();

  /**
   * 返回用于写操作的锁。
   *
   * @return 写锁实例。
   */
  public abstract Lock writeLock();
}
