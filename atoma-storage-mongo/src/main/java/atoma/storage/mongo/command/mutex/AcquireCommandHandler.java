package atoma.storage.mongo.command.mutex;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.HandlesCommand;
import atoma.api.coordination.command.LockCommand;
import atoma.storage.mongo.command.AtomaCollectionNamespace;
import atoma.storage.mongo.command.CommandExecutor;
import atoma.storage.mongo.command.CommandFailureException;
import atoma.storage.mongo.command.MongoCommandHandler;
import atoma.storage.mongo.command.MongoCommandHandlerContext;
import com.google.auto.service.AutoService;
import com.mongodb.MongoCommandException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

import static atoma.storage.mongo.command.MongoErrorCode.DUPLICATE_KEY;
import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;

/**
 * Handles the {@link LockCommand.Acquire} command to acquire a distributed mutex lock.
 *
 * <p>This handler implements a non-reentrant lock acquisition mechanism using a single, atomic
 * MongoDB {@code findOneAndUpdate} operation. The lock's state is stored in a MongoDB document.
 *
 * <h3>Acquisition Logic</h3>
 *
 * <p>The logic relies on an {@code upsert} operation with {@code $setOnInsert}:
 *
 * <ol>
 *   <li><b>If the lock does not exist:</b> A new document is created (upserted). The {@code
 *       $setOnInsert} operator sets the {@code holder} and {@code lease} fields to the requester's
 *       identifiers. The lock is successfully acquired.
 *   <li><b>If the lock already exists:</b> The {@code $setOnInsert} operator has no effect. The
 *       existing {@code holder} and {@code lease} fields are unchanged. The acquisition attempt
 *       will fail because the current requester is not the original holder that was set on insert.
 * </ol>
 *
 * <p>On every acquisition attempt, a {@code version} number in the document is incremented. This
 * version can be used as a fencing token to prevent operations with stale locks.
 *
 * <h3>MongoDB Document Schema</h3>
 *
 * <p>The lock state is stored in a document with the following structure:
 *
 * <pre>{@code
 * {
 *   "_id": "<resource-id>",
 *   "holder": "<holder-id>",
 *   "lease": "<lease-id>",
 *   "version": <long>
 * }
 * }</pre>
 *
 * <ul>
 *   <li>{@code _id}: The unique identifier for the locked resource.
 *   <li>{@code holder}: A unique identifier for the thread or process that holds the lock.
 *   <li>{@code lease}: The lease ID associated with the lock, used for expiration.
 *   <li>{@code version}: A numeric fencing token that is incremented on each acquisition attempt.
 * </ul>
 */
@SuppressWarnings("rawtypes")
@AutoService({CommandHandler.class})
@HandlesCommand(LockCommand.Acquire.class)
public final class AcquireCommandHandler
    extends MongoCommandHandler<LockCommand.Acquire, LockCommand.AcquireResult> {

  @Override
  public LockCommand.AcquireResult execute(
      LockCommand.Acquire command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection =
        getCollection(context, AtomaCollectionNamespace.MUTEX_LOCK_NAMESPACE);

    Function<ClientSession, LockCommand.AcquireResult> cmdBlock =
        session -> {

          // Which will return an old document if the lock is already held by other thread.
          Document lockDoc =
              collection.findOneAndUpdate(
                  eq("_id", context.getResourceId()),
                  combine(
                      setOnInsert("lease", command.leaseId()),
                      setOnInsert("holder", command.holderId()),
                      inc("version", 1L)),
                  new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).upsert(true));

          // Acquisition success.
          if (lockDoc != null
              && lockDoc.getString("holder").equals(command.holderId())
              && lockDoc.getString("lease").equals(command.leaseId()))
            return new LockCommand.AcquireResult(true, lockDoc.getLong("version"));

          // Acquisition failed.
          if (lockDoc != null) {
            return new LockCommand.AcquireResult(false, lockDoc.getLong("version"));
          }

          // The lock does not exist. But acquisition failed. In this scenario. It's an
          // unexpected error that external logic to retry.
          return new LockCommand.AcquireResult(false, -1L);
        };

    Result<LockCommand.AcquireResult> result =
        this.newCommandExecutor(client)
            .withoutTxn()
            .retryOnException(CommandFailureException.class)
            .retryOnException(
                throwable ->
                    throwable instanceof MongoCommandException cmdEx
                        && cmdEx.getCode() == DUPLICATE_KEY.getCode())
            .retryOnCode(WRITE_CONFLICT)
            .withTimeout(Duration.of(command.timeout(), command.timeUnit().toChronoUnit()))
            .execute(cmdBlock);

    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
