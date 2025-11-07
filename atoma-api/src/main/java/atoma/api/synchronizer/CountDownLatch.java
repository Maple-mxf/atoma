package atoma.api.synchronizer;

import atoma.api.Resourceful;

public interface CountDownLatch extends Resourceful {
  /** 执行向下计数 减1操作 */
  void countDown();

  /**
   * 挂起当前线程等待
   *
   * @throws InterruptedException 主线程deadline，wait状态的线程会抛出此错误
   */
  void await() throws InterruptedException;

  /**
   * @return 返回需要计数的基数
   */
  int getCount();

  /**
   * Deletes the latch resource from the backend coordination service.
   * This is useful for explicit resource cleanup.
   */
  void destroy();
}
