package atoma.api.coordination;

/**
 * Represents an active subscription to change events of a specific resource node
 * within the distributed coordination service.
 *
 * <p>The primary responsibility of a {@code Subscription} object is to manage
 * the lifecycle of the subscription, particularly providing the ability to
 * explicitly cancel it.
 *
 * <p>This interface extends {@link AutoCloseable}, making it suitable for use
 * in try-with-resources statements. This ensures that the subscription is
 * cleanly and automatically terminated when the managing scope exits.
 */
public interface Subscription extends AutoCloseable {
  /**
   * Cancels this subscription.
   *
   * <p>Once this method is called, the subscriber will stop receiving any
   * further notifications of resource node changes, and any underlying
   * resources (e.g., database change stream cursors) related to this
   * subscription will be released. This operation is idempotent.
   */
  void unsubscribe();

  /**
   * Checks if this subscription is currently active and has not been cancelled.
   *
   * @return {@code true} if the subscription is valid and active; {@code false} otherwise.
   */
  boolean isSubscribed();

  /**
   * Retrieves the unique key of the resource that this subscription is associated with.
   *
   * @return The unique identifier key of the subscribed resource.
   */
  String getResourceKey();

  /**
   * Implements the {@link AutoCloseable} interface.
   *
   * <p>When this {@code Subscription} object is used in a try-with-resources
   * statement, this method is automatically invoked upon exiting the try block.
   * Its behavior is equivalent to calling {@link #unsubscribe()}, ensuring that
   * the subscription is properly terminated and associated resources are released.
   */
  default void close() {
    unsubscribe();
  }
}
