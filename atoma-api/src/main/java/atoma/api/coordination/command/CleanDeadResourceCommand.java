package atoma.api.coordination.command;

/**
 * A container for commands related to cleaning up dead or expired resources
 * within the coordination service.
 */
public final class CleanDeadResourceCommand {

  /**
   * Represents a command to trigger the cleanup of dead resources.
   *
   * <p>This command signals the {@link atoma.api.coordination.CoordinationStore}
   * to identify and remove any resources that are no longer valid, such as
   * locks held by expired leases. It does not return any value upon completion.
   * @param leaseId The lease ID of the client.
   */
  public record Clean(String leaseId) implements Command<Void> {}
}
