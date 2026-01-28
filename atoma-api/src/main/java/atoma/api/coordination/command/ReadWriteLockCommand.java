package atoma.api.coordination.command;

import java.util.concurrent.TimeUnit;

/**
 * A container for all commands related to distributed {@link atoma.api.lock.ReadWriteLock}
 * operations.
 *
 * <p>This class encapsulates the distinct actions for acquiring and releasing shared (read) and
 * exclusive (write) locks. It reuses the result types defined in {@link LockCommand}.
 */
public final class ReadWriteLockCommand {

  private ReadWriteLockCommand() {}

  // --- Read Lock Commands ---

  /**
   * Command to acquire a shared read lock. Multiple read locks can be held simultaneously by
   * different parties. This supports re-entrancy.
   *
   * @param holderId A unique identifier for the party attempting to acquire the lock.
   * @param leaseId The lease ID of the client, ensuring the lock is released if the client fails.
   * @param timeout The maximum time to wait for the lock.
   * @param timeUnit The time unit for the timeout argument.
   */
  public record AcquireRead(String holderId, String leaseId, long timeout, TimeUnit timeUnit)
      implements Command<LockCommand.AcquireResult> {}

  /**
   * Command to release a previously acquired shared read lock.
   *
   * @param holderId A unique identifier for the party releasing the lock.
   * @param leaseId The lease ID of the client.
   */
  public record ReleaseRead(String holderId, String leaseId) implements Command<Void> {}

  // --- Write Lock Commands ---

  /**
   * Command to acquire an exclusive write lock. Only one write lock can be held at a time, and it
   * blocks all other read and write locks. This supports re-entrancy.
   *
   * @param holderId A unique identifier for the party attempting to acquire the lock.
   * @param leaseId The lease ID of the client, ensuring the lock is released if the client fails.
   * @param timeout The maximum time to wait for the lock.
   * @param timeUnit The time unit for the timeout argument.
   */
  public record AcquireWrite(String holderId, String leaseId, long timeout, TimeUnit timeUnit)
      implements Command<LockCommand.AcquireResult> {}

  /**
   * Command to release a previously acquired exclusive write lock.
   *
   * @param holderId A unique identifier for the party releasing the lock, which must match the
   *     identifier that acquired it.
   * @param leaseId The lease ID of the client, ensuring the lock is released if the client fails.
   */
  public record ReleaseWrite(String holderId, String leaseId) implements Command<Void> {}
}
