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
import com.google.auto.service.AutoService;
import com.mongodb.MongoCommandException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static atoma.storage.mongo.command.MongoErrorCode.DUPLICATE_KEY;
import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

/**
 * Handles the {@link ReadWriteLockCommand.AcquireRead} command to acquire a distributed, shared
 * read lock.
 *
 * <p>This handler implements read lock acquisition, allowing multiple readers to hold a lock
 * concurrently. It uses a single atomic MongoDB {@code findOneAndUpdate} operation. A read lock can
 * only be acquired if no write lock is currently held.
 *
 * <h3>Acquisition Logic</h3>
 *
 * <p>The operation succeeds if the lock document has no {@code write_lock} field. On successful
 * acquisition, the handler atomically adds a sub-document with the caller's {@code holder} and
 * {@code lease} identifiers to the {@code read_locks} array.
 *
 * <p>The update uses a {@code $setUnion} operation, which makes the acquisition idempotent: if the
 * caller already holds a read lock, the {@code read_locks} array remains unchanged. This provides a
 * form of re-entrancy without a counter. A top-level {@code version} field is also incremented on
 * each attempt.
 *
 * <h3>MongoDB Document Schema</h3>
 *
 * <p>The handler interacts with a document structured as follows:
 *
 * <pre>{@code
 * {
 *   "_id": "<rw-lock-resource-id>",
 *   "version": <long>,
 *   "write_lock": { ... }, // Exists only if a write lock is held
 *   "read_locks": [
 *     { "holder": "<holder-id-1>", "lease": "<lease-id-1>" },
 *     { "holder": "<holder-id-2>", "lease": "<lease-id-2>" },
 *     ...
 *   ]
 * }
 * }</pre>
 */
@SuppressWarnings("rawtypes")
@AutoService({CommandHandler.class})
@HandlesCommand(ReadWriteLockCommand.AcquireRead.class)
public class RLAcquireCommandHandler
    extends MongoCommandHandler<ReadWriteLockCommand.AcquireRead, LockCommand.AcquireResult> {

  @Override
  public LockCommand.AcquireResult execute(
      ReadWriteLockCommand.AcquireRead command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection =
        getCollection(context, AtomaCollectionNamespace.RW_LOCK_NAMESPACE);

    Function<ClientSession, LockCommand.AcquireResult> cmdBlock =
        session -> {

          // 1. Attempt lock acquisition
          // Return a duplicate-key exception because of does not match the condition.
          Document lockDoc;
          try {
            lockDoc =
                collection.findOneAndUpdate(
                    and(
                        eq("_id", context.getResourceId()),
                        exists("write_lock.holder", false),
                        exists("write_lock.lease", false)),
                    combine(
                        set(
                            "read_locks",
                            new Document(
                                "$cond",
                                new Document("if", new Document("$isArray", "$read_locks"))
                                    .append(
                                        "then",
                                        new Document(
                                            "$setUnion",
                                            List.of(
                                                "$read_locks",
                                                Collections.singletonList(
                                                    new Document("holder", command.holderId())
                                                        .append("lease", command.leaseId()))))))),
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
              && lockDoc.getList("read_locks", Document.class) != null
              && lockDoc.getList("read_locks", Document.class).stream()
                  .anyMatch(
                      t ->
                          t.getString("lease").equals(command.leaseId())
                              && t.getString("holder").equals(command.holderId()))) {
            return new LockCommand.AcquireResult(true, lockDoc.getLong("version"));
          }

          // Acquisition failed.
          if (lockDoc != null) {
            return new LockCommand.AcquireResult(false, lockDoc.getLong("version"));
          }

          // The lock does not exist,But the acquisition failed, In this scenario. It's an
          // unexpected error that external logic to retry.
          return new LockCommand.AcquireResult(false, -1L);
        };

    Result<LockCommand.AcquireResult> result =
        this.newCommandExecutor(client)
            .withoutTxn()
            .retryOnCode(WRITE_CONFLICT)
            .retryOnException(CommandFailureException.class)
            .execute(cmdBlock);

    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
