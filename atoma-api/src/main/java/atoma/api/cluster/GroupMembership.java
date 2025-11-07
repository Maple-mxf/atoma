package atoma.api.cluster;

import atoma.api.Leasable;
import atoma.api.Resourceful;

import java.util.List;

/**
 * Provides an interface for managing and monitoring membership within a distributed group. Members
 * join the group under a specific lease, and are automatically removed if their lease expires.
 */
public interface GroupMembership extends Leasable, AutoCloseable {

  /**
   * Returns a current snapshot of all members in the group.
   *
   * @return a list of current group members.
   */
  List<GroupMember> getMembers();

  /**
   * Adds a listener to receive notifications about changes in group membership.
   *
   * @param listener the listener to add.
   */
  void addListener(GroupListener listener);

  /**
   * Removes a previously added listener.
   *
   * @param listener the listener to remove.
   */
  void removeListener(GroupListener listener);

  /** Closes this group membership instance, causing this member to leave the group. */
  @Override
  void close();
}
