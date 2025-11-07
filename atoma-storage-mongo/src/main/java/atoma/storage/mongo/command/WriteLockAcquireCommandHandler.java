package atoma.storage.mongo.command;

import atoma.api.AtomaStateException;
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
 * Handles the acquisition of a distributed, re-entrant write lock.
 *
 * <p>It first attempts a re-entrant acquisition if the lock is already held by the same caller. If
 * not, it attempts to acquire a new lock by atomically setting the {@code write_lock} field,
 * conditioned on no other read or write locks existing.
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
 *   // This sub-document exists only when a write lock is held. Its presence acts as an exclusive lock.
 *   "write_lock": {
 *     "holder": "lease-abc:thread-1",
 *     "lease": "lease-abc",
 *     "reentrant_count": 1
 *   },
 *
 *   // --- Read Lock State ---
 *   // This sub-document exists only when one or more read locks are held.
 *   "read_locks": [
 *     {
 *       "holder": "thread-2",
 *       "lease": "lease-xyz",
 *       "reentrant_count": 2
 *     }
 *   ]
 * }
 * }</pre>
 *
 * <h4>Locking Rules:</h4>
 *
 * <ul>
 *   <li><b>Acquiring a Write Lock:</b> Succeeds only if both {@code write_lock} and {@code
 *       read_locks} fields do not exist.
 *   <li><b>Acquiring a Read Lock:</b> Succeeds only if the {@code write_lock} field does not exist.
 * </ul>
 */
@HandlesCommand(ReadWriteLockCommand.AcquireWrite.class)
public class WriteLockAcquireCommandHandler
    extends MongoCommandHandler<ReadWriteLockCommand.AcquireWrite, LockCommand.AcquireResult> {

  @Override
  public LockCommand.AcquireResult execute(
      ReadWriteLockCommand.AcquireWrite command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = client.getDatabase("atoma_db").getCollection("rw_locks");

    Function<ClientSession, LockCommand.AcquireResult> cmdBlock =
        session -> {
          // 1. Attempt re-entrant lock acquisition
          Document reentrantDoc =
              collection.findOneAndUpdate(
                  session,
                  and(
                      eq("_id", context.getResourceId()),
                      eq("write_lock.holder", command.holderId())),
                  inc("write_lock.reentrant_count", 1),
                  new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

          if (reentrantDoc != null) {
            return new LockCommand.AcquireResult(
                true, reentrantDoc.get("write_lock", Document.class).getInteger("reentrant_count"));
          }

          // 2. Attempt new lock acquisition
          // Condition: No write lock AND no read locks exist.
          Document newLockDoc =
              collection.findOneAndUpdate(
                  session,
                  and(
                      eq("_id", context.getResourceId()),
                      exists("write_lock", false),
                      exists("read_locks", false)),
                  combine(
                      set(
                          "write_lock",
                          new Document("holder", command.holderId())
                              .append("lease", command.leaseId())
                              .append("reentrant_count", 1))),
                  new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

          if (newLockDoc != null) {
            return new LockCommand.AcquireResult(true, 1);
          }

          // 3. Lock is held by others
          return new LockCommand.AcquireResult(false, 0);
        };

    Result<LockCommand.AcquireResult> result =
        this.newExecution(client).retryOnCode(WRITE_CONFLICT).execute(cmdBlock);

    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
