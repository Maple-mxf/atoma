package atoma.api.coordination.command;

import java.util.Optional;

/**
 * Defines commands for Leader Election.
 */
public final class LeaderElectionCommand {
  private LeaderElectionCommand() {}

  public record Campaign(String leaseId) implements Command<CampaignResult> {}
  public record Resign(String leaseId) implements Command<Void> {}
  public record GetLeader() implements Command<GetLeaderResult> {}

  public record CampaignResult(boolean isLeader) {}
  public record GetLeaderResult(Optional<String> leaderId) {}
}