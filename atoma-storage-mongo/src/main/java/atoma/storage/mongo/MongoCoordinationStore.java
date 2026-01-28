package atoma.storage.mongo;

import atoma.api.AtomaStateException;
import atoma.api.IllegalOwnershipException;
import atoma.api.Resourceful;
import atoma.api.Result;
import atoma.api.coordination.CoordinationStore;
import atoma.api.coordination.Resource;
import atoma.api.coordination.ResourceChangeEvent;
import atoma.api.coordination.ResourceListener;
import atoma.api.coordination.Subscription;
import atoma.api.coordination.command.Command;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.HandlesCommand;
import atoma.storage.mongo.command.CommandExecutor;
import atoma.storage.mongo.command.MongoCommandHandlerContext;
import atoma.storage.mongo.command.MongoErrorCode;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Supplier;

import static atoma.storage.mongo.command.AtomaCollectionNamespace.*;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.in;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MongoCoordinationStore implements CoordinationStore {

  private final MongoClient mongoClient;

  private final MongoDatabase mongoDatabase;

  private final Map<Class<? extends Command>, CommandHandler> commandHandlerRegistry =
      new ConcurrentHashMap<>();
  private final Map<String, List<ResourceListener>> listenerRegistry = new ConcurrentHashMap<>();
  private final Thread watcherThread;

  public MongoCoordinationStore(MongoClient mongoClient, String db) {
    this.mongoClient = mongoClient;
    this.mongoDatabase = mongoClient.getDatabase(db);
    this.checkLeaseIndex();

    // Discover and register all command handlers
    ServiceLoader.load(CommandHandler.class).forEach(this::registerHandler);

    // Watch the entire database for changes
    MongoCursor<ChangeStreamDocument<Document>> sharedCursor =
        mongoClient
            .getDatabase(db)
            .watch(
                singletonList(
                    match(
                        in(
                            "ns.coll",
                            LEASE_NAMESPACE,
                            DOUBLE_BARRIER_NAMESPACE,
                            BARRIER_NAMESPACE,
                            COUNTDOWN_LATCH_NAMESPACE,
                            SEMAPHORE_NAMESPACE,
                            MUTEX_LOCK_NAMESPACE,
                            RW_LOCK_NAMESPACE))))
            .fullDocument(FullDocument.UPDATE_LOOKUP)
            .fullDocumentBeforeChange(FullDocumentBeforeChange.WHEN_AVAILABLE)
            .cursor();

    this.watcherThread =
        new Thread(() -> demultiplexerLoop(sharedCursor), "atoma-event-demultiplexer");
    this.watcherThread.setDaemon(true);
    this.watcherThread.start();
  }

  private void checkLeaseIndex() {
    MongoCollection<Document> collection =
        mongoDatabase
            .getCollection(LEASE_NAMESPACE)
            .withReadConcern(CommandExecutor.READ_CONCERN)
            .withWriteConcern(CommandExecutor.WRITE_CONCERN);

    Function<ClientSession, Boolean> cmdBlock =
        session -> {
          Supplier<Boolean> indexFinder =
              () -> {
                List<Document> documentList = new ArrayList<>(4);
                collection.listIndexes().into(documentList);
                return documentList.stream()
                    .anyMatch(
                        t -> {
                          Document key = t.get("key", Document.class);
                          return key.containsKey("expire_time")
                              && t.containsKey("expireAfterSeconds");
                        });
              };

          if (indexFinder.get()) return true;
          collection.createIndex(
              Indexes.ascending("expire_time"),
              new IndexOptions().expireAfter(8L, SECONDS).name("expire_time"));
          return indexFinder.get();
        };

    Result<Boolean> result =
        new CommandExecutor<Boolean>(this.mongoClient)
            .withoutTxn()
            .retryOnResult(t -> !t)
            .retryOnCode(MongoErrorCode.EXCEEDED_TIME_LIMIT)
            .execute(cmdBlock);

    try {
      if (result.isSuccess() && !result.getOrThrow()) {
        throw new IllegalStateException(
            "Failed to check and create a TTL-index in MongoDB. This may be due to a or more data-bearing voting member has been down.");
      }
    } catch (Throwable e) {
      if (e instanceof IllegalOwnershipException) {
        throw (IllegalOwnershipException) e;
      }
      throw new AtomaStateException(e);
    }
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

  private ResourceChangeEvent buildChangeEventFrom(
      ChangeStreamDocument<Document> change, String resourceId) {
    var eventType =
        switch (Objects.requireNonNull(change.getOperationType())) {
          case DELETE -> ResourceChangeEvent.EventType.DELETED;
          case INSERT -> ResourceChangeEvent.EventType.CREATED;
          case UPDATE, REPLACE -> ResourceChangeEvent.EventType.UPDATED;
          default -> null;
        };

    if (eventType == null) return null;

    Resource oldNode =
        Optional.ofNullable(change.getFullDocumentBeforeChange())
            .map(BsonResource::new)
            .orElse(null);
    Resource newNode =
        Optional.ofNullable(change.getFullDocument()).map(BsonResource::new).orElse(null);

    return new ResourceChangeEvent(eventType, resourceId, newNode, oldNode);
  }

  @Override
  public Optional<Resource> get(String resourceId) {
    // This would need a proper implementation to fetch from the correct collection.
    return Optional.empty();
  }

  @Override
  public Subscription subscribe(
      Class<? extends Resourceful> resourceType, String resourceId, ResourceListener listener) {
    listenerRegistry.computeIfAbsent(resourceId, k -> new CopyOnWriteArrayList<>()).add(listener);
    return new MongoSubscription(
        resourceId,
        () ->
            listenerRegistry.computeIfPresent(
                resourceId,
                (k, v) -> {
                  v.remove(listener);
                  return v.isEmpty() ? null : v;
                }));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <R> R execute(String resourceId, Command<R> command) {
    CommandHandler<Command<R>, R> handler = commandHandlerRegistry.get(command.getClass());
    if (handler == null) {
      throw new AtomaStateException(
          "No command handler found for command: " + command.getClass().getName());
    }
    // In a real implementation, the context would be more sophisticated.
    MongoCommandHandlerContext context =
        new MongoCommandHandlerContext(mongoClient, mongoDatabase, resourceId);
    return handler.execute(command, context);
  }

  @Override
  public void close() {
    this.watcherThread.interrupt();
  }
}
