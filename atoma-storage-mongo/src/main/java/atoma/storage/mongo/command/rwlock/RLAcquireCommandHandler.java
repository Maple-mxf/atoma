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
import java.util.stream.Collectors;

import static atoma.storage.mongo.command.MongoErrorCode.DUPLICATE_KEY;
import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Filters.eq;
import static java.util.Collections.singletonList;

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

  /**
   * Builds the MongoDB aggregation pipeline for atomically acquiring a read lock.
   *
   * <p>The pipeline performs a conditional update. It checks if a write lock is held (i.e., if the
   * {@code write_lock} field is present and non-null).
   *
   * <ul>
   *   <li><b>If no write lock exists:</b> It adds the specified {@code owner} to the {@code
   *       read_locks} array using {@code $setUnion} for idempotency and increments the {@code
   *       version}. If the document or fields are new, they are created.
   *   <li><b>If a write lock exists:</b> The pipeline returns the document unmodified, causing the
   *       subsequent check to fail, thus preventing read lock acquisition.
   * </ul>
   *
   * @param owner A BSON {@link Document} representing the caller, containing {@code holder} and
   *     {@code lease} identifiers.
   * @return A {@link List} of {@link Bson} stages for the {@code findOneAndUpdate} operation.
   */
  private List<Bson> buildAggregationPipeline(Document owner) {
    List<Document> ownerDocArray = singletonList(owner);
    return List.of(
        new Document(
            "$replaceRoot",
            new Document(
                "newRoot",
                new Document(
                    "$cond",
                    Arrays.asList(
                        // ===== if =====
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
                                            Arrays.asList("$write_lock.holder", BsonNull.VALUE)))),
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
                                            Arrays.asList("$write_lock.lease", BsonNull.VALUE)))))),

                        // ===== then =====
                        new Document()
                            .append(
                                "read_locks",
                                new Document(
                                    "$cond",
                                    Arrays.asList(
                                        new Document("$isArray", "$read_locks"),
                                        new Document(
                                            "$setUnion",
                                            Arrays.asList("$read_locks", ownerDocArray)),
                                        ownerDocArray)))
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

                        // ===== else =====
                        // Fail-Soft
                        "$$ROOT")))));
  }

  /**
   * Executes the command to acquire a shared (read) lock on a resource.
   *
   * <p>This method attempts to atomically acquire a read lock using a {@code findOneAndUpdate}
   * operation. The operation is designed to be idempotent and will only succeed if no write lock is
   * currently held on the resource.
   *
   * <p>The process is wrapped in a retry mechanism that handles transient MongoDB errors like
   * {@code WriteConflict} and {@code DuplicateKey}. It also respects the timeout specified in the
   * command.
   *
   * @param command The {@link ReadWriteLockCommand.AcquireRead} command containing details like
   *     holder ID, lease ID, and timeout.
   * @param context The {@link MongoCommandHandlerContext} providing access to the client and
   *     resource details.
   * @return A {@link LockCommand.AcquireResult} where {@code success} is {@code true} if the lock
   *     was acquired and {@code false} otherwise. The result also includes the current lock
   *     version.
   * @throws OperationTimeoutException if the lock could not be acquired within the specified
   *     timeout.
   * @throws AtomaStateException if an unexpected error occurs during the operation.
   */
  @Override
  public LockCommand.AcquireResult execute(
      ReadWriteLockCommand.AcquireRead command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection =
        getCollection(context, AtomaCollectionNamespace.RW_LOCK_NAMESPACE);

    var owner = new Document("holder", command.holderId()).append("lease", command.leaseId());
    Function<ClientSession, LockCommand.AcquireResult> cmdBlock =
        session -> {

          // 1. Attempt lock acquisition
          // Return a duplicate-key exception because of does not match the condition.
          List<Bson> pipeline = this.buildAggregationPipeline(owner);

          Document lockDoc =
              collection.findOneAndUpdate(
                  eq("_id", context.getResourceId()),
                  pipeline,
                  new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

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
