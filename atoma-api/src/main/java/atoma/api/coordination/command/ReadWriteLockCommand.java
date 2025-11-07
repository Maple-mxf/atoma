package atoma.api.coordination.command;

import java.util.concurrent.TimeUnit;

/**
 * Defines the commands and result types for Read-Write Lock operations.
 */
public final class ReadWriteLockCommand {

  private ReadWriteLockCommand() {}

  // --- Read Lock Commands ---

  public record AcquireRead(String holderId, String leaseId, long timeout, TimeUnit timeUnit)
      implements Command<LockCommand.AcquireResult> {}

  public record ReleaseRead(String holderId, String leaseId) implements Command<LockCommand.ReleaseResult> {}

  // --- Write Lock Commands ---

  public record AcquireWrite(String holderId, String leaseId, long timeout, TimeUnit timeUnit)
      implements Command<LockCommand.AcquireResult> {}

  public record ReleaseWrite(String holderId) implements Command<LockCommand.ReleaseResult> {}
}
