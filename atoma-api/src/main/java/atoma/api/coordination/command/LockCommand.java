package atoma.api.coordination.command;

import java.util.concurrent.TimeUnit;

/**
 * A container for all commands and result types related to distributed reentrant {@link
 * atoma.api.lock.Lock} operations. This class encapsulates the actions for acquiring and releasing
 * a lock.
 */
public final class LockCommand {
  private LockCommand() {}

  // --- Result Objects ---

  /**
   * Represents the result of an {@link Acquire} command.
   *
   * @param acquired {@code true} if the lock was successfully acquired or re-entered.
   * @param serverLatestVersion The latest-version represent value that is advancing state-data version.
   *     If the state already exists, a value greater than 0 will be returned; otherwise, a negative
   *     value will be returned
   */
  public record AcquireResult(boolean acquired, long serverLatestVersion) {}

  /**
   * Represents the result of a {@link Release} command.
   *
   * @param stillHeld {@code true} if the lock is still held by the caller due to re-entrancy (i.e.,
   *     the lock was acquired multiple times).
   * @param remainingCount The remaining re-entrant acquisition count if the lock is still held.
   */
  @Deprecated
  public record ReleaseResult(boolean stillHeld, int remainingCount) {}

  // --- Commands ---

  /**
   * Command to acquire a distributed lock. This supports re-entrant locking.
   *
   * @param holderId A unique identifier for the party attempting to acquire the lock (e.g., thread
   *     ID).
   * @param leaseId The lease ID of the client, ensuring the lock is released if the client fails.
   * @param timeout The maximum time to wait for the lock.
   * @param timeUnit The time unit for the timeout argument.
   */
  public record Acquire(String holderId, String leaseId, long timeout, TimeUnit timeUnit)
      implements Command<AcquireResult> {}

  /**
   * Command to release a previously acquired distributed lock.
   *
   * @param holderId A unique identifier for the party releasing the lock, which must match the
   *     identifier that acquired it.
   */
  public record Release(String holderId) implements Command<Void> {}
}
