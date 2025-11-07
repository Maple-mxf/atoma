package atoma.storage.mongo.command;

import atoma.api.AtomaStateException;
import atoma.api.IllegalOwnershipException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandlerContext;
import atoma.api.coordination.command.HandlesCommand;
import atoma.api.coordination.command.LockCommand;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.function.Function;

import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.inc;

/**
 * Handles the release of a distributed, re-entrant mutex lock.
 *
 * <p>The release logic is atomic and handles two cases:
 *
 * <ol>
 *   <li><b>Decrement Re-entrant Count:</b> It first attempts to find the lock document where the
 *       caller is the holder and the {@code reentrant_count} is greater than 1. If found, it
 *       atomically decrements the count.
 *   <li><b>Full Release:</b> If the first step fails (meaning the count is 1), it attempts to find
 *       the document again and atomically deletes it, fully releasing the lock.
 * </ol>
 *
 * If neither operation affects a document, it means the caller was not the lock holder, and an
 * {@link atoma.api.IllegalOwnershipException} is thrown.
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
@HandlesCommand(LockCommand.Release.class)
public class MutexReleaseCommandHandler
    extends MongoCommandHandler<LockCommand.Release, LockCommand.ReleaseResult> {
  @Override
  public LockCommand.ReleaseResult execute(
      LockCommand.Release command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = client.getDatabase("atoma_db").getCollection("locks");

    Function<ClientSession, LockCommand.ReleaseResult> cmdBlock =
        session -> {
          Bson filter = and(eq("_id", context.getResourceId()), eq("holder", command.holderId()));

          Document updatedDoc =
              collection.findOneAndUpdate(
                  session,
                  and(filter, gt("reentrant_count", 1)),
                  inc("reentrant_count", -1),
                  new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

          if (updatedDoc != null) {
            // Lock is still held, but reentrancy is reduced.
            return new LockCommand.ReleaseResult(true, updatedDoc.getInteger("reentrant_count"));
          }

          DeleteResult deleteResult = collection.deleteOne(session, filter);

          if (deleteResult.getDeletedCount() == 1) {
            // Lock was fully released.
            return new LockCommand.ReleaseResult(false, 0);
          }

          throw new IllegalOwnershipException(
              "Cannot release lock for resource '"
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
