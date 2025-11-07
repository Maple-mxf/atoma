package atoma.core.internal.election;

import atoma.api.coordination.CoordinationStore;
import atoma.api.coordination.ResourceChangeEvent;
import atoma.api.coordination.Subscription;
import atoma.api.coordination.command.LeaderElectionCommand;
import atoma.api.election.LeaderElection;
import atoma.api.election.LeadershipListener;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultLeaderElection implements LeaderElection {

  private final String resourceId;
  private final String leaseId;
  private final CoordinationStore coordination;
  private final Subscription subscription;

  private final List<LeadershipListener> listeners = new CopyOnWriteArrayList<>();
  private final AtomicBoolean isCurrentlyLeader = new AtomicBoolean(false);

  public DefaultLeaderElection(String resourceId, String leaseId, CoordinationStore coordination) {
    this.resourceId = resourceId;
    this.leaseId = leaseId;
    this.coordination = coordination;

    // Check initial state
    updateLeadershipStatus(getLeaderId());

    this.subscription =
        coordination.subscribe(
            "", // TODO: Define constant for LeaderElection
            resourceId,
            this::handleLeadershipChange);
  }

  private void handleLeadershipChange(ResourceChangeEvent event) {
    Optional<String> newLeaderId = event.getNewNode().map(node -> node.get("leader_id"));
    updateLeadershipStatus(newLeaderId);
  }

  private void updateLeadershipStatus(Optional<String> currentLeaderId) {
    boolean amILeader = currentLeaderId.map(id -> id.equals(this.leaseId)).orElse(false);
    boolean wasILeader = isCurrentlyLeader.getAndSet(amILeader);

    if (amILeader && !wasILeader) {
      listeners.forEach(LeadershipListener::onElected);
    } else if (!amILeader && wasILeader) {
      listeners.forEach(LeadershipListener::onResigned);
    }
  }

  @Override
  public void campaign() {
    LeaderElectionCommand.CampaignResult result =
        coordination.execute(resourceId, new LeaderElectionCommand.Campaign(leaseId));
    updateLeadershipStatus(result.isLeader() ? Optional.of(leaseId) : getLeaderId());
  }

  @Override
  public void resign() {
    coordination.execute(resourceId, new LeaderElectionCommand.Resign(leaseId));
    updateLeadershipStatus(Optional.empty());
  }

  @Override
  public boolean isLeader() {
    return isCurrentlyLeader.get();
  }

  @Override
  public Optional<String> getLeaderId() {
    return coordination.execute(resourceId, new LeaderElectionCommand.GetLeader()).leaderId();
  }

  @Override
  public void addListener(LeadershipListener listener) {
    listeners.add(listener);
  }

  @Override
  public void removeListener(LeadershipListener listener) {
    listeners.remove(listener);
  }

  @Override
  public String getResourceId() {
    return resourceId;
  }

  @Override
  public void close() {
    resign();
    if (this.subscription != null) {
      this.subscription.close();
    }
  }

  @Override
  public String getLeaseId() {
    return leaseId;
  }
}
