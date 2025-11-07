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

import java.util.List;
import java.util.function.Function;

import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

/**
 * Handles the release of a distributed, re-entrant read lock based on an array schema.
 *
 * <p>The release logic is atomic and handles two cases for a given reader:
 *
 * <ol>
 *   <li><b>Decrement Re-entrant Count:</b> It first attempts to find the caller's sub-document in
 *       the {@code read_locks} array where its {@code reentrant_count} is greater than 1. If found,
 *       it atomically decrements the count.
 *   <li><b>Full Release of Reader:</b> If the count is 1, it instead performs an update using the
 *       {@code $pull} operator to atomically remove the caller's sub-document from the {@code
 *       read_locks} array, effectively releasing the read lock for that specific reader.
 * </ol>
 *
 * If no entry for the caller is found in the {@code read_locks} array, it means the caller does not
 * hold the lock, and an {@link atoma.api.IllegalOwnershipException} is thrown.
 *
 * <h3>MongoDB Document Schema for Read-Write Lock</h3>
 *
 * <p>The state of a distributed read-write lock is stored in a single document. The {@code
 * read_locks} field is an array of documents, which allows for atomic, queryable updates based on
 * lease IDs.
 *
 * <pre>{@code
 * {
 *   "_id": "rw-lock-resource-id",
 *   "write_lock": {
 *     "holder": "lease-abc:thread-1",
 *     "lease": "lease-abc",
 *     "reentrant_count": 1
 *   },
 *   "read_locks": [
 *     { "holder": "thread-1", "lease": "lease-abc", "reentrant_count": 2 },
 *     { "holder": "thread-5", "lease": "lease-xyz", "reentrant_count": 1 }
 *   ]
 * }
 * }</pre>
 */
@HandlesCommand(ReadWriteLockCommand.ReleaseRead.class)
public class ReadLockReleaseCommandHandler
    extends MongoCommandHandler<ReadWriteLockCommand.ReleaseRead, LockCommand.ReleaseResult> {

  @Override
  public LockCommand.ReleaseResult execute(
      ReadWriteLockCommand.ReleaseRead command, MongoCommandHandlerContext context) {
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
                      elemMatch(
                          "read_locks",
                          and(
                              eq("holder", command.holderId()),
                              eq("lease", command.leaseId()),
                              gt("reentrant_count", 1)))),
                  inc("read_locks.$.reentrant_count", -1),
                  new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

          if (updatedDoc != null) {
            List<Document> readers = updatedDoc.getList("read_locks", Document.class);
            int newCount =
                readers.stream()
                    .filter(
                        doc ->
                            command.holderId().equals(doc.getString("holder"))
                                && command.leaseId().equals(doc.getString("lease")))
                    .findFirst()
                    .map(doc -> doc.getInteger("reentrant_count"))
                    .orElse(0);
            return new LockCommand.ReleaseResult(true, newCount);
          }

          // 2. Attempt to fully release the lock for this reader (when count is 1)
          Document releasedDoc =
              collection.findOneAndUpdate(
                  session,
                  and(
                      eq("_id", context.getResourceId()),
                      elemMatch(
                          "read_locks",
                          and(eq("holder", command.holderId()), eq("lease", command.leaseId())))),
                  pull(
                      "read_locks",
                      and(eq("holder", command.holderId()), eq("lease", command.leaseId()))));

          if (releasedDoc != null) {
            return new LockCommand.ReleaseResult(false, 0);
          }

          // 3. If no document was affected, caller does not hold the lock
          throw new IllegalOwnershipException(
              "Cannot release read lock for resource '"
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
