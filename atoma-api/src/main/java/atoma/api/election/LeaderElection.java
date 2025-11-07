package atoma.api.election;

import atoma.api.Leasable;
import atoma.api.Resourceful;

import java.util.Optional;

/**
 * Provides an interface for participating in a distributed leader election.
 */
public interface LeaderElection extends Leasable, AutoCloseable {

  /**
   * Attempts to become the leader. This is a non-blocking operation.
   * <p>
   * If this client is already the leader, this call will simply confirm it.
   * If another client is the leader, this call will have no effect.
   * If there is no leader, this client will enter the election and attempt to acquire leadership.
   */
  void campaign();

  /**
   * Voluntarily gives up leadership, allowing other candidates to be elected.
   * If this client is not the leader, this call has no effect.
   */
  void resign();

  /**
   * Checks if this client is currently the leader.
   *
   * @return {@code true} if this client is the leader, {@code false} otherwise.
   */
  boolean isLeader();

  /**
   * Gets the ID of the current leader, if one exists.
   *
   * @return an Optional containing the leader's ID, or an empty Optional if there is no leader.
   */
  Optional<String> getLeaderId();

  /**
   * Adds a listener to receive notifications about leadership state changes.
   *
   * @param listener the listener to add.
   */
  void addListener(LeadershipListener listener);

  /**
   * Removes a previously added listener.
   *
   * @param listener the listener to remove.
   */
  void removeListener(LeadershipListener listener);

  /**
   * Closes this election instance, causing this client to withdraw from the election.
   * If it is the current leader, it will resign leadership.
   */
  @Override
  void close();
}