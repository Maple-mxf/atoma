package atoma.storage.mongo.command.rwlock;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.HandlesCommand;
import atoma.api.coordination.command.LockCommand;
import atoma.api.coordination.command.ReadWriteLockCommand;
import atoma.storage.mongo.command.AtomaCollectionNamespace;
import atoma.storage.mongo.command.CommandFailureException;
import atoma.storage.mongo.command.MongoCommandHandler;
import atoma.storage.mongo.command.MongoCommandHandlerContext;
import atoma.storage.mongo.command.MongoErrorCode;
import com.google.auto.service.AutoService;
import com.mongodb.MongoCommandException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;

import java.util.Optional;
import java.util.function.Function;

import static atoma.storage.mongo.command.MongoErrorCode.DUPLICATE_KEY;
import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

/**
 * Handles the {@link ReadWriteLockCommand.AcquireWrite} command to acquire a distributed,
 * exclusive write lock.
 *
 * <p>This handler implements a non-reentrant write lock acquisition. It uses a single atomic
 * MongoDB {@code findOneAndUpdate} operation to ensure that a write lock is granted only when no
 * other locks (neither read nor write) are currently held on the resource.
 *
 * <h3>Acquisition Logic</h3>
 *
 * <p>A write lock is successfully acquired if and only if the target resource document meets the
 * following criteria at the time of the operation:
 * <ol>
 *   <li>The document has no existing write lock (the {@code write_lock} field does not exist).</li>
 *   <li>The document has no existing read locks (the {@code read_locks} field either does not exist
 *       or is an empty array).</li>
 * </ol>
 *
 * <p>If these conditions are met, the operation atomically creates the {@code write_lock}
 * sub-document, setting the holder and lease identifiers. It also increments a top-level {@code
 * version} field, which can be used as a fencing token.
 *
 * <p><b>Note:</b> This implementation is non-reentrant. A thread that already holds a write lock
 * cannot acquire it again.
 *
 * <h3>MongoDB Document Schema</h3>
 *
 * <p>The state of a distributed read-write lock is stored in a single document with the following
 * structure:
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
@HandlesCommand(ReadWriteLockCommand.AcquireWrite.class)
public class WLAcquireCommandHandler
    extends MongoCommandHandler<ReadWriteLockCommand.AcquireWrite, LockCommand.AcquireResult> {

  @Override
  public LockCommand.AcquireResult execute(
      ReadWriteLockCommand.AcquireWrite command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection =
        getCollection(context, AtomaCollectionNamespace.RW_LOCK_NAMESPACE);

    Function<ClientSession, LockCommand.AcquireResult> cmdBlock =
        session -> {
          // 1. Attempt lock acquisition
          // Return a duplicate-key exception because of does not match the condition.
          Document lockDoc = null;
          try {
            lockDoc =
                collection.findOneAndUpdate(
                    and(
                        eq("_id", context.getResourceId()),
                        exists("write_lock.holder", false),
                        exists("write_lock.lease", false),
                        or(exists("read_locks", false), size("read_locks", 0))),
                    combine(
                        set("write_lock.holder", command.holderId()),
                        set("write_lock.lease", command.leaseId()),
                        inc("version", 1L)),
                    new FindOneAndUpdateOptions()
                        .upsert(true)
                        .returnDocument(ReturnDocument.AFTER));
          }
          // Fallback logic
          catch (MongoCommandException e) {
            if (DUPLICATE_KEY.getCode() == e.getErrorCode()) {
              // Find latest lock-document
              lockDoc =
                  collection
                      .find(eq("_id", context.getResourceId()), Document.class)
                      .projection(new Document("version", 1))
                      .first();
              return new LockCommand.AcquireResult(
                  false, Optional.ofNullable(lockDoc).map(t -> t.getLong("version")).orElse(-1L));
            }
            throw e;
          }

            // Acquisition success.
          if (lockDoc != null
              && lockDoc.containsKey("write_lock")
              && lockDoc
                  .get("write_lock", Document.class)
                  .getString("lease")
                  .equals(command.leaseId())
              && lockDoc
                  .get("write_lock", Document.class)
                  .getString("holder")
                  .equals(command.holderId())) {
            return new LockCommand.AcquireResult(
                true, lockDoc.get("write_lock", Document.class).getLong("version"));
          }

          // Acquisition failed.
          if (lockDoc != null) {
            return new LockCommand.AcquireResult(false, lockDoc.getLong("version"));
          }

          // The lock is not exists. but acquisition failed. In this scenario. It's an
          // unexpected error that external logic to retry.
          return new LockCommand.AcquireResult(false, -1L);
        };

    Result<LockCommand.AcquireResult> result =
        this.newCommandExecutor(client)
            .withoutTxn()
            .retryOnCode(WRITE_CONFLICT)
            .execute(cmdBlock);

    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
