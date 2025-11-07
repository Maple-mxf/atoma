package atoma.core.internal.cluster;

import atoma.api.cluster.GroupListener;
import atoma.api.cluster.GroupMember;
import atoma.api.cluster.GroupMembership;
import atoma.api.coordination.CoordinationStore;
import atoma.api.coordination.ResourceChangeEvent;
import atoma.api.coordination.Subscription;
import atoma.api.coordination.command.GroupMembershipCommand;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DefaultGroupMembership implements GroupMembership {

  private final String resourceId;
  private final String leaseId;
  private final CoordinationStore coordination;
  private final Subscription subscription;

  private final List<GroupListener> listeners = new CopyOnWriteArrayList<>();
  private volatile List<GroupMember> currentMembers = List.of();

  public DefaultGroupMembership(
      String resourceId,
      String leaseId,
      Map<String, String> metadata,
      CoordinationStore coordination) {
    this.resourceId = resourceId;
    this.leaseId = leaseId;
    this.coordination = coordination;

    // 1. Join the group immediately upon creation.
    coordination.execute(resourceId, new GroupMembershipCommand.Join(leaseId, metadata));

    // 2. Fetch initial state.
    this.currentMembers = getMembers();

    // 3. Subscribe to subsequent changes.
    this.subscription =
        coordination.subscribe(
            "", // TODO: Define constant for GroupMembership
            resourceId,
            this::handleMembershipChange);
  }

  private void handleMembershipChange(ResourceChangeEvent event) {
    List<GroupMember> oldMembers = this.currentMembers;

    List<GroupMember> newMembers =
        event
            .getNewNode()
            .map(
                node -> {
                  Object membersObj = node.get("members");
                  if (membersObj instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> membersList = (List<Map<String, Object>>) membersObj;
                    return membersList.stream()
                        .map(data -> (GroupMember) new MapGroupMember(data))
                        .collect(Collectors.toList());
                  }
                  return List.<GroupMember>of();
                })
            .orElse(List.of());

    this.currentMembers = newMembers;

    Map<String, GroupMember> oldMemberMap =
        oldMembers.stream().collect(Collectors.toMap(GroupMember::getId, Function.identity()));
    Map<String, GroupMember> newMemberMap =
        newMembers.stream().collect(Collectors.toMap(GroupMember::getId, Function.identity()));

    // Calculate who was removed
    for (GroupMember oldMember : oldMembers) {
      if (!newMemberMap.containsKey(oldMember.getId())) {
        listeners.forEach(listener -> listener.onMemberRemoved(oldMember));
      }
    }

    // Calculate who was added
    for (GroupMember newMember : newMembers) {
      if (!oldMemberMap.containsKey(newMember.getId())) {
        listeners.forEach(listener -> listener.onMemberAdded(newMember));
      }
    }
  }

  @Override
  public List<GroupMember> getMembers() {
    try {
      return coordination.execute(resourceId, new GroupMembershipCommand.GetMembers()).members();
    } catch (Exception e) {
      // In case of error, return the last known list.
      return currentMembers;
    }
  }

  @Override
  public void addListener(GroupListener listener) {
    listeners.add(listener);
  }

  @Override
  public void removeListener(GroupListener listener) {
    listeners.remove(listener);
  }

  @Override
  public String getResourceId() {
    return resourceId;
  }

  @Override
  public void close() {
    coordination.execute(resourceId, new GroupMembershipCommand.Leave(leaseId));
    if (this.subscription != null) {
      this.subscription.close();
    }
  }

  @Override
  public String getLeaseId() {
    return leaseId;
  }
}
