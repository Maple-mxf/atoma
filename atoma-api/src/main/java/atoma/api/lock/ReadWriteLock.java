package atoma.api.lock;

public interface ReadWriteLock extends AutoCloseable {

  /**
   * 返回用于读操作的锁。
   *
   * @return 读锁实例。
   */
  ReadLock readLock();

  /**
   * 返回用于写操作的锁。
   *
   * @return 写锁实例。
   */
  WriteLock writeLock();
}
