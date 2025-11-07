package atoma.api.coordination.command;

/**
 * Defines the commands and result types for DoubleCyclicBarrier operations.
 */
public final class DoubleCyclicBarrierCommand {

  private DoubleCyclicBarrierCommand() {}

  public record EnterResult(boolean passed) {}
  public record LeaveResult(boolean passed) {}

  public record Enter(int parties, String participantId, String leaseId) implements Command<EnterResult> {}
  public record Leave(int parties, String participantId, String leaseId) implements Command<LeaveResult> {}

}
