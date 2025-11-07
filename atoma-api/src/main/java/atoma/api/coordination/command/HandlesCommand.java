package atoma.api.coordination.command;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

/** 用于在Command {@link CommandHandler} 处理器上的标记支持的{@link Command} */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(value = {TYPE})
public @interface HandlesCommand {

  /**
   * @return 当前处理器支持的Command
   */
  Class<? extends Command> value();
}
