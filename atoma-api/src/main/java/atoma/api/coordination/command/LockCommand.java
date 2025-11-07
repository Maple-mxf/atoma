package atoma.api.coordination.command;

import java.util.concurrent.TimeUnit;

public final class LockCommand {
  private LockCommand() {}

  // --- Result Objects ---

  /**
   * The result of an Acquire command.
   * @param acquired true if the lock was successfully acquired or re-entered.
   * @param reentrantCount The new reentrancy count if acquired, otherwise undefined.
   */
  public record AcquireResult(boolean acquired, int reentrantCount) {}

  /**
   * The result of a Release command.
   * @param stillHeld true if the lock is still held by the caller due to reentrancy.
   * @param remainingCount The remaining reentrancy count if still held.
   */
  public record ReleaseResult(boolean stillHeld, int remainingCount) {}


  // --- Commands ---

  /**
   * Command to acquire a lock.
   */
  public record Acquire(String holderId, String leaseId, long timeout, TimeUnit timeUnit)
      implements Command<AcquireResult> {}

  /**
   * Command to release a lock.
   */
  public record Release(String holderId) implements Command<ReleaseResult> {}
}