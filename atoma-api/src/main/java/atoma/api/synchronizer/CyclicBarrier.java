package atoma.api.synchronizer;

import atoma.api.BrokenBarrierException;
import atoma.api.Resourceful;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @see java.util.concurrent.CyclicBarrier
 */
public abstract class CyclicBarrier extends Resourceful {

  public abstract void await(long timeout, TimeUnit unit)
      throws InterruptedException, BrokenBarrierException, TimeoutException;

  public abstract void await() throws InterruptedException, BrokenBarrierException;

  public abstract void reset();

  public abstract boolean isBroken();

  public abstract int getParties();

  public abstract int getNumberWaiting();
}
