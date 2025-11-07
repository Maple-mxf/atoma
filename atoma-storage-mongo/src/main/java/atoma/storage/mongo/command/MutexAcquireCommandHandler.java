package atoma.storage.mongo.command;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandlerContext;
import atoma.api.coordination.command.HandlesCommand;
import atoma.api.coordination.command.LockCommand;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

import java.time.Duration;
import java.util.function.Function;

import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;

/**
 * Handles the acquisition of a distributed, re-entrant mutex lock.
 *
 * <p>The acquisition logic is atomic and handles two cases:
 *
 * <ol>
 *   <li><b>New Acquisition:</b> It uses an {@code updateOne} operation with {@code upsert=true} and
 *       {@code $setOnInsert}. This atomically creates the lock document *only if* it does not
 *       already exist.
 *   <li><b>Re-entrant Acquisition:</b> If the {@code updateOne} operation did not perform an upsert
 *       (meaning the lock document already existed), it then attempts a {@code findOneAndUpdate}
 *       operation. This second operation will only succeed if the caller is the current lock
 *       holder, in which case it atomically increments the {@code reentrant_count}.
 * </ol>
 *
 * If both operations fail to grant the lock, it means the lock is held by another party.
 *
 * <h3>MongoDB Document Schema for Mutex Lock</h3>
 *
 * <p>The state of a distributed, re-entrant mutex lock is stored in a single document with the
 * following structure.
 *
 * <pre>{@code
 * {
 *   "_id": "mutex-resource-id",
 *   "holder": "lease-abc:thread-1",
 *   "lease": "lease-abc",
 *   "reentrant_count": 1
 * }
 * }</pre>
 *
 * <ul>
 *   <li><b>_id</b>: The unique ID of the resource being locked.
 *   <li><b>holder</b>: A unique identifier for the thread holding the lock.
 *   <li><b>lease</b>: The ID of the client session lease, used for expiration and cleanup.
 *   <li><b>reentrant_count</b>: An integer tracking how many times the holder has re-entrantly
 *       acquired the lock.
 * </ul>
 */
@HandlesCommand(LockCommand.Acquire.class)
public final class MutexAcquireCommandHandler
    extends MongoCommandHandler<LockCommand.Acquire, LockCommand.AcquireResult> {

  @Override
  public LockCommand.AcquireResult execute(
      LockCommand.Acquire command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = client.getDatabase("atoma_db").getCollection("locks");

    Function<ClientSession, LockCommand.AcquireResult> cmdBlock =
        session -> {
          var initialUpdateResult =
              collection.updateOne(
                  session,
                  eq("_id", context.getResourceId()),
                  combine(
                      setOnInsert("lease", command.leaseId()),
                      setOnInsert("holder", command.holderId()),
                      setOnInsert("reentrant_count", 1),
                      setOnInsert("version", 1L)),
                  new UpdateOptions().upsert(true));

          if (initialUpdateResult.getUpsertedId() != null) {
            // Successfully acquired the lock for the first time.
            return new LockCommand.AcquireResult(true, 1);
          }

          // Lock already existed, try to re-enter.
          var reentrantUpdateResult =
              collection.findOneAndUpdate(
                  session,
                  and(
                      eq("_id", context.getResourceId()),
                      eq("lease", command.leaseId()),
                      eq("holder", command.holderId())),
                  combine(inc("reentrant_count", 1), inc("version", 1L)),
                  new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

          if (reentrantUpdateResult != null) {
            // Successfully re-entered the lock.
            return new LockCommand.AcquireResult(
                true, reentrantUpdateResult.getInteger("reentrant_count"));
          }

          // Lock is held by another holder.
          return new LockCommand.AcquireResult(false, 0);
        };

    ExecutionBuilder<LockCommand.AcquireResult> execution =
        this.newExecution(client).retryOnCode(WRITE_CONFLICT);

    if (command.timeout() > 0 && command.timeUnit() != null) {
      execution.withTimeout(Duration.of(command.timeout(), command.timeUnit().toChronoUnit()));
    }

    Result<LockCommand.AcquireResult> result = execution.execute(cmdBlock);

    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
