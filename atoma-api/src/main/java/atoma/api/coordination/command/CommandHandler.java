package atoma.api.coordination.command;

/**
 * A command handler that executes a specific type of Command.
 *
 * @param <C> The type of Command this handler can process.
 * @param <R> The type of Result this command produces.
 */
@FunctionalInterface
public interface CommandHandler<C extends Command<R>, R> {

  /**
   * Handles a command in the given context.
   *
   * @param command The command to handle.
   * @param context The context providing resources for execution, like the database session.
   * @return A command-specific result object.
   */
  R execute(C command, CommandHandlerContext context);
}