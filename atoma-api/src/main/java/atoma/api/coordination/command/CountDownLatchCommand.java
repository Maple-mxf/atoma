package atoma.api.coordination.command;

/**
 * A container for all commands and result types related to distributed
 * {@link atoma.api.synchronizer.CountDownLatch} operations. This class
 * encapsulates the different actions that can be performed on a latch resource.
 */
public final class CountDownLatchCommand {

  private CountDownLatchCommand() {}

  // --- Result Objects ---

  /**
   * Represents the result of a {@link GetCount} command, containing the current
   * count of the latch.
   *
   * @param count The current count of the latch.
   */
  public record GetCountResult(int count) {}

  // --- Commands ---

  /**
   * Command to initialize a distributed latch with a specific count.
   * This operation is typically conditional, setting the count only if the
   * latch resource does not already exist.
   *
   * @param count The initial count for the latch.
   */
  public record Initialize(int count) implements Command<Void> {}

  /**
   * Command to decrement the latch count by one. If the count reaches zero,
   * this may trigger the release of waiting parties.
   */
  public record CountDown() implements Command<Void> {}

  /**
   * Command to retrieve the current count of the latch.
   *
   * @return A {@link GetCountResult} containing the current count.
   */
  public record GetCount() implements Command<GetCountResult> {}

  /**
   * Command to permanently delete the latch resource from the backend storage.
   * This is used for explicit resource cleanup.
   */
  public record Destroy() implements Command<Void> {}
}