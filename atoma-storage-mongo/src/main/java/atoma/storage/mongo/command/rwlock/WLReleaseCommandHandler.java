package atoma.storage.mongo.command.rwlock;

import atoma.api.AtomaStateException;
import atoma.api.IllegalOwnershipException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.HandlesCommand;
import atoma.api.coordination.command.ReadWriteLockCommand;
import atoma.storage.mongo.command.AtomaCollectionNamespace;
import atoma.storage.mongo.command.CommandFailureException;
import atoma.storage.mongo.command.MongoCommandHandler;
import atoma.storage.mongo.command.MongoCommandHandlerContext;
import com.google.auto.service.AutoService;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.function.Function;

import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Filters.*;

/**
 * Handles the {@link ReadWriteLockCommand.ReleaseWrite} command to release a distributed, exclusive
 * write lock.
 *
 * <p>This handler releases a previously acquired write lock. It validates ownership and atomically
 * removes the write lock information from the corresponding MongoDB document.
 *
 * <h3>Release Logic</h3>
 *
 * <p>The handler finds the lock document where the {@code write_lock.holder} matches the caller's
 * ID. Upon finding it, it atomically removes the entire {@code write_lock} sub-document via the
 * {@code $unset} operator. This releases the lock, allowing other readers or writers to proceed.
 *
 * <p>If no document is found for the specified holder, the handler throws an {@link
 * atoma.api.IllegalOwnershipException}, as the caller does not own the lock.
 *
 * <p><b>Note:</b> The implementation contains a legacy code path for re-entrant locks. However, as
 * the corresponding {@link WLAcquireCommandHandler} creates non-reentrant locks, this handler
 * functions as a standard non-reentrant lock release.
 *
 * <h3>MongoDB Document Schema</h3>
 *
 * <p>The handler interacts with a document structured as follows (note the absence of {@code
 * reentrant_count}):
 *
 * <pre>{@code
 * {
 *   "_id": "<rw-lock-resource-id>",
 *   "version": <long>,
 *
 *   // --- Write Lock State ---
 *   // This sub-document exists only when a write lock is held. Its presence acts as an exclusive lock.
 *   "write_lock": {
 *     "holder": "<holder-id>",
 *     "lease": "<lease-id>"
 *   },
 *
 *   // --- Read Lock State ---
 *   // This array exists and is non-empty only when one or more read locks are held.
 *   "read_locks": [
 *     { "holder": "<holder-id>", "lease": "<lease-id>" },
 *     ...
 *   ]
 * }
 * }</pre>
 */
@SuppressWarnings("rawtypes")
@AutoService({CommandHandler.class})
@HandlesCommand(ReadWriteLockCommand.ReleaseWrite.class)
public class WLReleaseCommandHandler
    extends MongoCommandHandler<ReadWriteLockCommand.ReleaseWrite, Void> {

  @Override
  public Void execute(
      ReadWriteLockCommand.ReleaseWrite command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection =
        getCollection(context, AtomaCollectionNamespace.RW_LOCK_NAMESPACE);

    Function<ClientSession, Void> cmdBlock =
        session -> {
          Bson filter =
              and(
                  eq("_id", context.getResourceId()),
                  eq("write_lock.holder", command.holderId()),
                  eq("write_lock.lease", command.leaseId()));
          DeleteResult deleteResult = collection.deleteOne(filter);

          if (deleteResult.getDeletedCount() == 1L) return null;

          throw new IllegalOwnershipException(
              "Cannot release write lock for resource '"
                  + context.getResourceId()
                  + "' because it is not held by holder '"
                  + command.holderId()
                  + "'");
        };

    Result<Void> result =
        this.newCommandExecutor(client)
            .withoutTxn()
            .retryOnException(CommandFailureException.class)
            .retryOnCode(WRITE_CONFLICT)
            .execute(cmdBlock);

    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      if (e instanceof IllegalOwnershipException) {
        throw (IllegalOwnershipException) e;
      }
      throw new AtomaStateException(e);
    }
  }
}
