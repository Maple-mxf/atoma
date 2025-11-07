package atoma.storage.mongo.command;

import atoma.api.AtomaStateException;
import atoma.api.IllegalOwnershipException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandlerContext;
import atoma.api.coordination.command.HandlesCommand;
import atoma.api.coordination.command.LockCommand;
import atoma.api.coordination.command.ReadWriteLockCommand;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;

import java.util.function.Function;

import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

/**
 * Handles the release of a distributed, re-entrant write lock.
 *
 * <p>It first attempts to decrement the re-entrant count. If the count drops to zero, it fully
 * releases the lock by unsetting the {@code write_lock} field. Throws {@link
 * atoma.api.IllegalOwnershipException} if the caller does not hold the lock.
 *
 * <p>
 *
 * <h3>MongoDB Document Schema for Read-Write Lock</h3>
 *
 * <p>The state of a distributed read-write lock is stored in a single document with the following
 * structure. This schema is designed to allow for atomic updates for all read/write lock
 * operations.
 *
 * <pre>{@code
 * {
 *   "_id": "rw-lock-resource-id",
 *
 *   // --- Write Lock State ---
 *   "write_lock": {
 *     "holder": "lease-abc:thread-1",
 *     "lease": "lease-abc",
 *     "reentrant_count": 1
 *   },
 *
 *   // --- Read Lock State ---
 *   "read_locks": [
 *     {
 *       "holder": "thread-2",
 *       "lease": "lease-xyz",
 *       "reentrant_count": 1
 *     }
 *   ]
 * }
 * }</pre>
 */
@HandlesCommand(ReadWriteLockCommand.ReleaseWrite.class)
public class WriteLockReleaseCommandHandler
    extends MongoCommandHandler<ReadWriteLockCommand.ReleaseWrite, LockCommand.ReleaseResult> {

  @Override
  public LockCommand.ReleaseResult execute(
      ReadWriteLockCommand.ReleaseWrite command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = client.getDatabase("atoma_db").getCollection("rw_locks");

    Function<ClientSession, LockCommand.ReleaseResult> cmdBlock =
        session -> {
          // 1. Attempt to decrement re-entrant count
          Document updatedDoc =
              collection.findOneAndUpdate(
                  session,
                  and(
                      eq("_id", context.getResourceId()),
                      eq("write_lock.holder", command.holderId()),
                      gt("write_lock.reentrant_count", 1)),
                  inc("write_lock.reentrant_count", -1),
                  new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

          if (updatedDoc != null) {
            return new LockCommand.ReleaseResult(
                true, updatedDoc.get("write_lock", Document.class).getInteger("reentrant_count"));
          }

          // 2. Attempt to fully release the lock (when count is 1)
          Document releasedDoc =
              collection.findOneAndUpdate(
                  session,
                  and(
                      eq("_id", context.getResourceId()),
                      eq("write_lock.holder", command.holderId())),
                  unset("write_lock"));

          if (releasedDoc != null) {
            return new LockCommand.ReleaseResult(false, 0);
          }

          // 3. If no document was found, caller does not hold the lock
          throw new IllegalOwnershipException(
              "Cannot release write lock for resource '"
                  + context.getResourceId()
                  + "' because it is not held by holder '"
                  + command.holderId()
                  + "'");
        };

    Result<LockCommand.ReleaseResult> result =
        this.newExecution(client).retryOnCode(WRITE_CONFLICT).execute(cmdBlock);

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
