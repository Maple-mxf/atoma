package atoma.api.coordination.command;

/**
 * A container for all commands and result types related to distributed
 * {@link atoma.api.synchronizer.CyclicBarrier} operations. This class
 * encapsulates the different actions that can be performed on a barrier resource.
 */
public final class CyclicBarrierCommand {

  private CyclicBarrierCommand() {}

  // --- Result Objects ---

  /**
   * Represents the result of an {@link Await} command.
   *
   * @param passed {@code true} if this awaiting party was the last one required to trip the barrier,
   *               {@code false} otherwise.
   * @param broken {@code true} if the barrier was broken either before or as a result of this await.
   */
  public record AwaitResult(boolean passed, boolean broken) {}

  /**
   * Represents the state of the barrier, returned by a {@link GetState} command.
   *
   * @param parties       The number of parties required to trip this barrier.
   * @param numberWaiting The number of parties currently waiting at the barrier.
   * @param isBroken      {@code true} if the barrier is in a broken state.
   * @param generation    The current generation of the barrier. A generation changes when the
   *                      barrier is tripped or reset.
   * @param version       The internal version of the resource, used for optimistic locking.
   */
  public record GetStateResult(int parties, int numberWaiting, boolean isBroken, long generation, long version) {}

  // --- Commands ---

  /**
   * Command for a party to await on the barrier.
   *
   * @param parties       The number of parties required to trip the barrier. This is used to
   *                      initialize the barrier on the first await.
   * @param participantId A unique identifier for the awaiting party to prevent duplicate awaits
   *                      within the same generation.
   * @param leaseId       The lease ID of the client, ensuring that if the client fails, its
   *                      participation can be timed out.
   */
  public record Await(
      int parties,
      String participantId,
      String leaseId)
      implements Command<AwaitResult> {}

  /**
   * Command to reset the barrier to its initial state. This is useful for re-using a barrier
   * after it has been tripped or broken.
   *
   * @param expectedVersion The expected version of the resource for an optimistic lock,
   *                        ensuring the reset doesn't conflict with other operations.
   */
  public record Reset(long expectedVersion) implements Command<Void> {}

  /**
   * Command to retrieve the current state of the barrier.
   * This is primarily used for monitoring and debugging.
   */
  public record GetState() implements Command<GetStateResult> {}
}