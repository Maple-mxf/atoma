package atoma.api.synchronizer;

import atoma.api.Resourceful;

public abstract class DoubleCyclicBarrier extends Resourceful {

  public abstract int getParticipants();

  public abstract void enter() throws InterruptedException;

  public abstract void leave() throws InterruptedException;
}
