package atoma.api.lock;

import atoma.api.Leasable;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface Lock extends Leasable, AutoCloseable {

  void lock() throws InterruptedException;

  void lock(long time, TimeUnit unit) throws InterruptedException, TimeoutException;

  void lockInterruptibly() throws InterruptedException;

  void unlock();
}
