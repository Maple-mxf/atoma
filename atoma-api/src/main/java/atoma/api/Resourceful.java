package atoma.api;

import java.util.concurrent.atomic.AtomicBoolean;

/** Resourceful of class. */
public abstract class Resourceful implements AutoCloseable {

  protected final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * @return The resource unique-id
   */
  public abstract String getResourceId();

  /**
   * @return The current resource is closed.
   */
  public boolean isClosed() {
    return closed.get();
  }
}
