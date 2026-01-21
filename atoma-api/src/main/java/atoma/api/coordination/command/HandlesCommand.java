package atoma.api.coordination.command;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

/**
 * An annotation used on a {@link CommandHandler} implementation to declare
 * which specific {@link Command} class it is responsible for handling.
 *
 * <p>This allows for the automatic registration and discovery of command handlers
 * for their corresponding commands.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(value = {TYPE})
public @interface HandlesCommand {

  /**
   * Specifies the {@link Command} class that the annotated handler can process.
   *
   * @return The command class supported by the handler.
   */
  Class<? extends Command> value();
}
