package atoma.api.synchronizer;

import atoma.api.Resourceful;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @see java.util.concurrent.CyclicBarrier
 */
public interface CyclicBarrier extends Resourceful {

  void await(long timeout, TimeUnit unit) throws InterruptedException, BrokenBarrierException, TimeoutException;

  void await() throws InterruptedException, BrokenBarrierException;

  void reset();

  boolean isBroken();

  int getParties();

  int getNumberWaiting();
}
