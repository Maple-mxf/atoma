package atoma.api.lock;

import atoma.api.Leasable;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class Lock extends Leasable {

  public abstract void lock() throws InterruptedException;

  public abstract void lock(long time, TimeUnit unit) throws InterruptedException, TimeoutException;

  public abstract void lockInterruptibly() throws InterruptedException;

  public abstract void unlock();
}
