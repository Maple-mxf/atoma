package atoma.api.cluster;

/**
 * A listener for receiving notifications about group membership changes.
 */
public interface GroupListener {

  /**
   * Fired when a new member joins the group.
   *
   * @param member the member that joined.
   */
  void onMemberAdded(GroupMember member);

  /**
   * Fired when a member leaves the group, either gracefully or due to lease expiration.
   *
   * @param member the member that left.
   */
  void onMemberRemoved(GroupMember member);
}