package atoma.api.coordination.command;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

public class CommandHandlerRegistry {

  public Map<Class<? extends Command>, CommandHandler<? extends Command>> discoverAndRegister() {
    Iterator<CommandHandler> cmdIterator = ServiceLoader.load(CommandHandler.class).iterator();

    Map<Class<? extends Command>, CommandHandler<?>> registry = new HashMap<>();
    while (cmdIterator.hasNext()) {
      CommandHandler commandHandler = cmdIterator.next();
      registry.put((Class<? extends Command>) commandHandler.getClass(), commandHandler);
    }
    return registry;
  }
}
