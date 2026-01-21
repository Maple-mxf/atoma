package atoma.api.coordination.command;

import java.util.concurrent.TimeUnit;

/**
 * A container for all commands and result types related to distributed
 * {@link atoma.api.synchronizer.Semaphore} operations. This class encapsulates
 * the actions for acquiring and releasing permits.
 */
public final class SemaphoreCommand {

  private SemaphoreCommand() {}

  // --- Result Objects ---

  /**
   * Represents the result of an {@link Acquire} command.
   *
   * @param acquired {@code true} if the requested permits were successfully acquired.
   */
  public record AcquireResult(boolean acquired) {}

  // --- Commands ---

  /**
   * Command to acquire a specified number of permits from the semaphore.
   *
   * @param permits        The number of permits to acquire.
   * @param holderId       A unique identifier for the party attempting to acquire permits.
   * @param leaseId        The lease ID of the client, ensuring permits are released if the client fails.
   * @param timeout        The maximum time to wait to acquire the permits.
   * @param timeUnit       The time unit for the timeout argument.
   * @param initialPermits The total number of permits the semaphore should have. This is used to
   *                       conditionally initialize the semaphore on its first use.
   */
  public record Acquire(
      int permits,
      String holderId,
      String leaseId,
      long timeout,
      TimeUnit timeUnit,
      int initialPermits)
      implements Command<AcquireResult> {}

  /**
   * Command to release a specified number of permits back to the semaphore.
   *
   * @param permits   The number of permits to release.
   * @param holderId  A unique identifier for the party releasing the permits.
   * @param leaseId   The lease ID of the client.
   */
  public record Release(int permits, String holderId, String leaseId)
      implements Command<Void> {}
}
