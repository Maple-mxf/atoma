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

import java.util.List;
import java.util.function.Function;

import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

/**
 * Handles the acquisition of a distributed, re-entrant read lock based on an array schema.
 *
 * <p>The logic is executed atomically and handles two primary cases:
 *
 * <ol>
 *   <li><b>Re-entrant Acquisition:</b> It first attempts to find a document where the caller
 *       (identified by holder and lease ID) already exists in the {@code read_locks} array. If
 *       found, it atomically increments the {@code reentrant_count} for that specific reader using
 *       the positional {@code $} operator.
 *   <li><b>New Acquisition:</b> If the caller is not yet a reader, it attempts to acquire a new
 *       read lock. This operation succeeds only if no write lock is held (the {@code write_lock}
 *       field does not exist). On success, it atomically adds a new sub-document for the reader to
 *       the {@code read_locks} array using the {@code $push} operator.
 * </ol>
 *
 * If a write lock exists when trying a new acquisition, the operation fails.
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
@HandlesCommand(ReadWriteLockCommand.AcquireRead.class)
public class ReadLockAcquireCommandHandler
    extends MongoCommandHandler<ReadWriteLockCommand.AcquireRead, LockCommand.AcquireResult> {

  @Override
  public LockCommand.AcquireResult execute(
      ReadWriteLockCommand.AcquireRead command, MongoCommandHandlerContext context) {
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
                      elemMatch(
                          "read_locks",
                          and(eq("holder", command.holderId()), eq("lease", command.leaseId())))),
                  inc("read_locks.$.reentrant_count", 1),
                  new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

          if (reentrantDoc != null) {
            // Find the specific reader's document to get the new count.
            List<Document> readers = reentrantDoc.getList("read_locks", Document.class);
            int newCount =
                readers.stream()
                    .filter(
                        doc ->
                            command.holderId().equals(doc.getString("holder"))
                                && command.leaseId().equals(doc.getString("lease")))
                    .findFirst()
                    .map(doc -> doc.getInteger("reentrant_count"))
                    .orElse(0); // Should not happen if re-entry succeeds
            return new LockCommand.AcquireResult(true, newCount);
          }

          // 2. Attempt new lock acquisition
          Document newLockDoc =
              collection.findOneAndUpdate(
                  session,
                  and(eq("_id", context.getResourceId()), exists("write_lock", false)),
                  push(
                      "read_locks",
                      new Document("holder", command.holderId())
                          .append("lease", command.leaseId())
                          .append("reentrant_count", 1)),
                  new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

          if (newLockDoc != null) {
            return new LockCommand.AcquireResult(true, 1);
          }

          // 3. A write lock exists, acquisition fails.
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
