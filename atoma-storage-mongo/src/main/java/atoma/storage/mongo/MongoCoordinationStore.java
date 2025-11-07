package atoma.storage.mongo;

import atoma.api.AtomaStateException;
import atoma.api.coordination.CoordinationStore;
import atoma.api.coordination.Resource;
import atoma.api.coordination.ResourceChangeEvent;
import atoma.api.coordination.ResourceListener;
import atoma.api.coordination.Subscription;
import atoma.api.coordination.command.Command;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.HandlesCommand;
import atoma.storage.mongo.command.MongoCommandHandlerContext;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class MongoCoordinationStore implements CoordinationStore {

  private final MongoClient mongoClient;
  private final String dbName;

  private final Map<Class<? extends Command>, CommandHandler> commandHandlerRegistry = new ConcurrentHashMap<>();
  private final Map<String, List<ResourceListener>> listenerRegistry = new ConcurrentHashMap<>();
  private final Thread watcherThread;

  public MongoCoordinationStore(MongoClient mongoClient, String db) {
    this.mongoClient = mongoClient;
    this.dbName = db;

    // Discover and register all command handlers
    ServiceLoader.load(CommandHandler.class).forEach(this::registerHandler);

    // Watch the entire database for changes
    MongoCursor<ChangeStreamDocument<Document>> sharedCursor = mongoClient.getDatabase(db)
        .watch()
        .fullDocument(FullDocument.UPDATE_LOOKUP)
        .fullDocumentBeforeChange(FullDocumentBeforeChange.WHEN_AVAILABLE)
        .cursor();

    this.watcherThread = new Thread(() -> demultiplexerLoop(sharedCursor), "atoma-event-demultiplexer");
    this.watcherThread.setDaemon(true);
    this.watcherThread.start();
  }

  private void registerHandler(CommandHandler handler) {
    HandlesCommand annotation = handler.getClass().getAnnotation(HandlesCommand.class);
    if (annotation != null) {
      commandHandlerRegistry.put(annotation.value(), handler);
    }
  }

  private void demultiplexerLoop(MongoCursor<ChangeStreamDocument<Document>> cursor) {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        ChangeStreamDocument<Document> change = cursor.next();
        BsonDocument documentKey = change.getDocumentKey();
        if (documentKey == null) continue;

        String resourceId = documentKey.getString("_id").getValue();
        List<ResourceListener> interestedListeners = listenerRegistry.get(resourceId);

        if (interestedListeners == null || interestedListeners.isEmpty()) continue;

        ResourceChangeEvent event = buildChangeEventFrom(change, resourceId);
        if (event == null) continue;

        interestedListeners.forEach(listener -> listener.onEvent(event));
      } catch (Exception e) {
        // In a real application, add proper logging and error handling/recovery.
      }
    }
  }

  private ResourceChangeEvent buildChangeEventFrom(ChangeStreamDocument<Document> change, String resourceId) {
    var eventType = switch (Objects.requireNonNull(change.getOperationType())) {
      case DELETE -> ResourceChangeEvent.EventType.DELETED;
      case INSERT -> ResourceChangeEvent.EventType.CREATED;
      case UPDATE, REPLACE -> ResourceChangeEvent.EventType.UPDATED;
      default -> null;
    };

    if (eventType == null) return null;

    Resource oldNode = Optional.ofNullable(change.getFullDocumentBeforeChange()).map(BsonResource::new).orElse(null);
    Resource newNode = Optional.ofNullable(change.getFullDocument()).map(BsonResource::new).orElse(null);

    return new ResourceChangeEvent(eventType, resourceId, newNode, oldNode);
  }

  @Override
  public Optional<Resource> get(String resourceId) {
    // This would need a proper implementation to fetch from the correct collection.
    return Optional.empty();
  }

  @Override
  public Subscription subscribe(String resourceType, String resourceId, ResourceListener listener) {
    listenerRegistry.computeIfAbsent(resourceId, k -> new CopyOnWriteArrayList<>()).add(listener);
    return new MongoSubscription(resourceId, () ->
        listenerRegistry.computeIfPresent(resourceId, (k, v) -> {
          v.remove(listener);
          return v.isEmpty() ? null : v;
        }));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <R> R execute(String resourceId, Command<R> command) {
    CommandHandler<Command<R>, R> handler = commandHandlerRegistry.get(command.getClass());
    if (handler == null) {
      throw new AtomaStateException("No command handler found for command: " + command.getClass().getName());
    }
    // In a real implementation, the context would be more sophisticated.
    MongoCommandHandlerContext context = new MongoCommandHandlerContext(mongoClient, dbName, resourceId);
    return handler.execute(command, context);
  }

  @Override
  public void close() {
    this.watcherThread.interrupt();
  }
}