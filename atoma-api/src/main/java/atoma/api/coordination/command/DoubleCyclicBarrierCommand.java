package atoma.api.coordination.command;

/**
 * A container for commands and result types for distributed {@link atoma.api.synchronizer.DoubleCyclicBarrier}
 * operations.
 *
 * <p>This type of barrier involves two synchronization phases: an 'enter' phase where parties
 * arrive at the barrier, and a 'leave' phase where parties depart.
 */
public final class DoubleCyclicBarrierCommand {

  private DoubleCyclicBarrierCommand() {}

  /**
   * Represents the result of an {@link Enter} command.
   *
   * @param passed {@code true} if this party was the last one required to complete the 'enter' phase.
   */
  public record EnterResult(boolean passed) {}

  /**
   * Represents the result of a {@link Leave} command.
   *
   * @param passed {@code true} if this party was the last one required to complete the 'leave' phase.
   */
  public record LeaveResult(boolean passed) {}

  /**
   * Command for a party to enter the barrier (the first phase).
   *
   * @param parties       The number of parties required to trip the barrier. This is used to
   *                      initialize the barrier on the first command.
   * @param participantId A unique identifier for the awaiting party to prevent duplicate entries.
   * @param leaseId       The lease ID of the client, ensuring that if the client fails, its
   *                      participation can be timed out.
   */
  public record Enter(int parties, String participantId, String leaseId) implements Command<EnterResult> {}

  /**
   * Command for a party to leave the barrier (the second phase).
   *
   * @param parties       The number of parties required to trip the barrier.
   * @param participantId A unique identifier for the awaiting party to prevent duplicate leaves.
   * @param leaseId       The lease ID of the client.
   */
  public record Leave(int parties, String participantId, String leaseId) implements Command<LeaveResult> {}

}
