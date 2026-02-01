package atoma.storage.mongo.command.rwlock;

import atoma.api.AtomaStateException;
import atoma.api.OperationTimeoutException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.HandlesCommand;
import atoma.api.coordination.command.LockCommand;
import atoma.api.coordination.command.ReadWriteLockCommand;
import atoma.storage.mongo.command.AtomaCollectionNamespace;
import atoma.storage.mongo.command.MongoCommandHandler;
import atoma.storage.mongo.command.MongoCommandHandlerContext;
import com.google.auto.service.AutoService;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import dev.failsafe.TimeoutExceededException;
import org.bson.BsonNull;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static atoma.storage.mongo.command.MongoErrorCode.DUPLICATE_KEY;
import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Aggregates.replaceRoot;
import static com.mongodb.client.model.Filters.eq;

/**
 * Handles the {@link ReadWriteLockCommand.AcquireWrite} command to acquire a distributed, exclusive
 * write lock.
 *
 * <p>This handler implements a non-reentrant write lock acquisition. It uses a single atomic
 * MongoDB {@code findOneAndUpdate} operation to ensure that a write lock is granted only when no
 * other locks (neither read nor write) are currently held on the resource.
 *
 * <h3>Acquisition Logic</h3>
 *
 * <p>A write lock is successfully acquired if and only if the target resource document meets the
 * following criteria at the time of the operation:
 *
 * <ol>
 *   <li>The document has no existing write lock (the {@code write_lock} field does not exist).
 *   <li>The document has no existing read locks (the {@code read_locks} field either does not exist
 *       or is an empty array).
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
  private List<Bson> buildAggregationPipeline(Document owner) {
    return List.of(
        replaceRoot(
            new Document(
                "newRoot",
                new Document(
                    "$cond",
                    Arrays.asList(

                        // ================= if =================
                        new Document(
                            "$and",
                            Arrays.asList(

                                // ---- read_locks missing OR size == 0 ----
                                new Document(
                                    "$or",
                                    Arrays.asList(
                                        new Document(
                                            "$eq",
                                            Arrays.asList(
                                                new Document("$type", "$read_locks"), "missing")),
                                        new Document(
                                            "$cond",
                                            Arrays.asList(
                                                new Document(
                                                    "$eq",
                                                    Arrays.asList(
                                                        new Document("$size", "$read_locks"), 0)),
                                                true,
                                                false)))),

                                // ---- write_lock.holder & lease missing or null ----
                                new Document(
                                    "$and",
                                    Arrays.asList(
                                        new Document(
                                            "$or",
                                            Arrays.asList(
                                                new Document(
                                                    "$eq",
                                                    Arrays.asList(
                                                        new Document("$type", "$write_lock.holder"),
                                                        "missing")),
                                                new Document(
                                                    "$eq",
                                                    Arrays.asList(
                                                        "$write_lock.holder", BsonNull.VALUE)))),
                                        new Document(
                                            "$or",
                                            Arrays.asList(
                                                new Document(
                                                    "$eq",
                                                    Arrays.asList(
                                                        new Document("$type", "$write_lock.lease"),
                                                        "missing")),
                                                new Document(
                                                    "$eq",
                                                    Arrays.asList(
                                                        "$write_lock.lease", BsonNull.VALUE)))))))),

                        // ================= then =================
                        new Document()
                            .append("write_lock", owner)
                            .append(
                                "version",
                                new Document(
                                    "$cond",
                                    Arrays.asList(
                                        new Document(
                                            "$or",
                                            Arrays.asList(
                                                new Document(
                                                    "$eq",
                                                    Arrays.asList(
                                                        new Document("$type", "$version"),
                                                        "missing")),
                                                new Document(
                                                    "$eq",
                                                    Arrays.asList("$version", BsonNull.VALUE)))),
                                        1L,
                                        new Document("$add", Arrays.asList("$version", 1L))))),

                        // ================= else =================
                        "$$ROOT")))));
  }

  @Override
  public LockCommand.AcquireResult execute(
      ReadWriteLockCommand.AcquireWrite command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection =
        getCollection(context, AtomaCollectionNamespace.RW_LOCK_NAMESPACE);

    var owner = new Document("holder", command.holderId()).append("lease", command.leaseId());
    List<Bson> pipeline = this.buildAggregationPipeline(owner);
    Function<ClientSession, LockCommand.AcquireResult> cmdBlock =
        session -> {
          // 1. Attempt lock acquisition
          // Return a duplicate-key exception because of does not match the condition.
          Document lockDoc =
              collection.findOneAndUpdate(
                  eq("_id", context.getResourceId()),
                  pipeline,
                  new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

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
            return new LockCommand.AcquireResult(true, lockDoc.getLong("version"));
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
            .withoutCausallyConsistent()
            .retryOnCode(WRITE_CONFLICT)
            .retryOnCode(DUPLICATE_KEY)
            .withTimeout(Duration.of(command.timeout(), command.timeUnit().toChronoUnit()))
            .execute(cmdBlock);

    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      // Translate Exception
      if (e instanceof TimeoutExceededException timeoutEx) {
        throw new OperationTimeoutException(timeoutEx);
      }
      throw new AtomaStateException(e);
    }
  }
}
