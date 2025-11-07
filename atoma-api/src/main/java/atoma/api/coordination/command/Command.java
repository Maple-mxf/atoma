package atoma.api.coordination.command;

import java.io.Serializable;

/**
 * A command to be executed against a resource.
 *
 * @param <R> The type of the result returned by this command.
 */
public interface Command<R> extends Serializable {}