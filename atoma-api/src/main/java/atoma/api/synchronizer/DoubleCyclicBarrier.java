package atoma.api.synchronizer;

import atoma.api.Resourceful;

/** 分布式Double Barrier实现 当参与者进入enter状态时，执行线程挂起，当所有参与者都处于enter状态时， 执行线程唤醒 */
public abstract class DoubleCyclicBarrier extends Resourceful {

  /**
   * @return 返回参与者的数量
   */
  public abstract int getParticipants();

  /**
   * 参与者进入enter状态，线程挂起，当所有参与者都到达时，线程会被唤醒
   *
   * @throws InterruptedException 主线程deadline，wait状态的线程会抛出此错误
   */
  public abstract void enter() throws InterruptedException;

  /**
   * 参与者进入leave状态，线程挂起，当所有参与者都到达时，线程会被唤醒
   *
   * @throws InterruptedException 主线程deadline，wait状态的线程会抛出此错误
   */
  public abstract void leave() throws InterruptedException;
}
