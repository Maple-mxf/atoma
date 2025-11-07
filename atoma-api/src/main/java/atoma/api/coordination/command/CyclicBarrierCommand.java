package atoma.api.coordination.command;

import atoma.api.synchronizer.BrokenBarrierException;

import java.util.concurrent.TimeUnit;

/**
 * Defines the commands and result types for CyclicBarrier operations.
 */
public final class CyclicBarrierCommand {

  private CyclicBarrierCommand() {}

  // --- Result Objects ---

  public record AwaitResult(boolean passed, boolean broken) {}

  public record GetStateResult(int parties, int numberWaiting, boolean isBroken, long generation, long version) {}

  // --- Commands ---

  public record Await(
      int parties,
      String participantId,
      String leaseId)
      implements Command<AwaitResult> {}

  public record Reset(long expectedVersion) implements Command<Void> {}

  public record GetState() implements Command<GetStateResult> {}
}