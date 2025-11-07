package atoma.api.election;

/**
 * A listener for receiving notifications about leadership changes relevant to this client.
 */
public interface LeadershipListener {

  /**
   * Fired when this client becomes the leader.
   * The client should now start performing its leadership duties.
   */
  void onElected();

  /**
   * Fired when this client loses its leadership status.
   * This can happen due to a graceful resignation, or because the underlying lease expired.
   * The client should immediately stop all leadership duties.
   */
  void onResigned();
}