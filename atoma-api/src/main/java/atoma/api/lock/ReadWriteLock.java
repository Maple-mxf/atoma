package atoma.api.lock;

import atoma.api.Leasable;

public abstract class ReadWriteLock extends Leasable {

  public abstract Lock readLock();

  public abstract Lock writeLock();
}
