package atoma.api.coordination.command;

import atoma.api.cluster.GroupMember;

import java.util.List;
import java.util.Map;

/**
 * Defines commands for Group Membership.
 */
public final class GroupMembershipCommand {
  private GroupMembershipCommand() {}

  public record Join(String leaseId, Map<String, String> metadata) implements Command<Void> {}
  public record Leave(String leaseId) implements Command<Void> {}
  public record GetMembers() implements Command<GetMembersResult> {}

  public record GetMembersResult(List<GroupMember> members) {}
}