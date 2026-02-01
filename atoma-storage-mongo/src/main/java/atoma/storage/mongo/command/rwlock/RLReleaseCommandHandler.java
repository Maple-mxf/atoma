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
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;

import java.util.function.Function;

import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.pull;

/**
 * Handles the {@link ReadWriteLockCommand.ReleaseRead} command to release a distributed, shared
 * read lock.
 *
 * <p><b>Warning:</b> This handler does not correctly release the read lock. It verifies that the
 * caller holds a read lock but fails to remove the caller's entry from the {@code read_locks}
 * array.
 *
 * <h3>Actual Behavior</h3>
 *
 * <p>The handler executes a {@code findOneAndUpdate} operation that locates the lock document if
 * the caller's {@code holder} and {@code lease} identifiers are present in the {@code read_locks}
 * array.
 *
 * <p>If a matching document is found, the operation only increments a top-level {@code version}
 * field. It <b>does not</b> remove the caller's sub-document from the {@code read_locks} array,
 * meaning the read lock is never actually released.
 *
 * <p>If no document is found matching the caller's identifiers, the handler throws an {@link
 * atoma.api.IllegalOwnershipException}.
 */
@SuppressWarnings("rawtypes")
@AutoService({CommandHandler.class})
@HandlesCommand(ReadWriteLockCommand.ReleaseRead.class)
public class RLReleaseCommandHandler
    extends MongoCommandHandler<ReadWriteLockCommand.ReleaseRead, Void> {

  @Override
  public Void execute(
      ReadWriteLockCommand.ReleaseRead command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection =
        getCollection(context, AtomaCollectionNamespace.RW_LOCK_NAMESPACE);

    Function<ClientSession, Void> cmdBlock =
        session -> {
          // 1. Attempt to release
          Document lockDoc =
              collection.findOneAndUpdate(
                  and(
                      eq("_id", context.getResourceId()),
                      elemMatch(
                          "read_locks",
                          and(eq("holder", command.holderId()), eq("lease", command.leaseId())))),
                  combine(
                      inc("version", 1),
                      pull(
                          "read_locks",
                          new Document("holder", command.holderId())
                              .append("lease", command.leaseId()))),
                  new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

          if (lockDoc != null
              && (lockDoc.getList("read_locks", Document.class) == null
                  || lockDoc.getList("read_locks", Document.class).stream()
                      .noneMatch(
                          t ->
                              t.getString("lease").equals(command.leaseId())
                                  && t.getString("holder").equals(command.holderId())))) {
            return null;
          }

          // 2. If no document was affected, the caller does not hold the lock
          throw new IllegalOwnershipException(
              "Cannot release read lock for resource '"
                  + context.getResourceId()
                  + "' because it is not held by holder '"
                  + command.holderId()
                  + "'");
        };

    Result<Void> result =
        this.newCommandExecutor(client)
            .withoutTxn()
            .withoutCausallyConsistent()
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
