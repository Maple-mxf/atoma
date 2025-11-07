package atoma.api.coordination.command;

/**
 * Defines the commands and result types for CountDownLatch operations.
 */
public final class CountDownLatchCommand {

  private CountDownLatchCommand() {}

  // --- Result Objects ---

  public record GetCountResult(int count) {}

  // --- Commands ---

  /**
   * Command to initialize the latch with a specific count, only if it doesn't exist.
   */
  public record Initialize(int count) implements Command<Void> {}

  /**
   * Command to decrement the latch count.
   */
  public record CountDown() implements Command<Void> {}

  /**
   * Command to get the current count of the latch.
   */
  public record GetCount() implements Command<GetCountResult> {}

  /**
   * Command to permanently delete the latch resource from the backend.
   */
  public record Destroy() implements Command<Void> {}
}