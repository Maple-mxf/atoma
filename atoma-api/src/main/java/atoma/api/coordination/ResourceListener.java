package atoma.api.coordination;

/**
 * A functional callback interface for receiving state change events of a {@link Resource}.
 *
 * <p>Implementations of this interface are typically registered via
 * {@link CoordinationStore#subscribe(String, String, ResourceListener)} to listen for
 * updates, creations, or deletions of specific distributed resources.
 * Being a functional interface, it can be implemented concisely using lambda expressions.
 */
@FunctionalInterface
public interface ResourceListener {

  /**
   * Called when a state change event occurs for the subscribed resource.
   *
   * @param event The {@link ResourceChangeEvent} object containing detailed information
   *              about the event, including the type of change and the new/old resource state.
   */
  void onEvent(ResourceChangeEvent event);
}
