package atoma.api.coordination.command;

import java.util.concurrent.TimeUnit;

/** Defines the commands and result types for Semaphore operations. */
public final class SemaphoreCommand {

  private SemaphoreCommand() {}

  // --- Result Objects ---

  public record AcquireResult(boolean acquired) {}

  // --- Commands ---

  public record Acquire(
      int permits,
      String holderId,
      String leaseId,
      long timeout,
      TimeUnit timeUnit,
      int initialPermits) // The total number of permits if the semaphore is being created.
      implements Command<AcquireResult> {}

  public record Release(int permits, String holderId, String leaseId)
      implements Command<Void> {}
}
