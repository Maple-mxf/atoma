package atoma.storage.mongo.command.mutex;

import atoma.api.AtomaStateException;
import atoma.api.IllegalOwnershipException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.HandlesCommand;
import atoma.api.coordination.command.LockCommand;
import atoma.storage.mongo.command.AtomaCollectionNamespace;
import atoma.storage.mongo.command.MongoCommandHandler;
import atoma.storage.mongo.command.MongoCommandHandlerContext;
import com.google.auto.service.AutoService;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.function.Function;

import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Filters.*;

/**
 * Handles the {@link LockCommand.Release} command to release a distributed mutex lock.
 *
 * <p><b>Warning:</b> This implementation contains logic based on a {@code reentrant_count} field,
 * which is inconsistent with the lock creation logic in {@code AcquireCommandHandler}. As a result,
 * this command may not function as expected.
 *
 * <h3>Release Logic</h3>
 *
 * <p>The handler attempts to release a lock by executing a {@code deleteOne} operation with a
 * filter that requires:
 *
 * <ol>
 *   <li>The resource ID matches.
 *   <li>The holder ID matches.
 *   <li>A field named {@code reentrant_count} has a value greater than 1.
 * </ol>
 *
 * <p>If a document is successfully deleted under these conditions, the release is considered
 * successful.
 *
 * <p>However, if no document is deleted (which will be the case for locks created by the current
 * {@code AcquireCommandHandler} as it does not set a {@code reentrant_count}), the handler throws
 * an {@link atoma.api.IllegalOwnershipException}. This effectively means the lock release will
 * always fail for locks acquired through the standard mechanism.
 */
@SuppressWarnings("rawtypes")
@AutoService({CommandHandler.class})
@HandlesCommand(LockCommand.Release.class)
public class ReleaseCommandHandler extends MongoCommandHandler<LockCommand.Release, Void> {
  @Override
  public Void execute(LockCommand.Release command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection =
        getCollection(context, AtomaCollectionNamespace.MUTEX_LOCK_NAMESPACE);

    Function<ClientSession, Void> cmdBlock =
        session -> {
          Bson filter = and(eq("_id", context.getResourceId()), eq("holder", command.holderId()));
          DeleteResult deleteResult = collection.deleteOne(filter);

          if (deleteResult.getDeletedCount() == 1L) return null;

          throw new IllegalOwnershipException(
              "Cannot release lock for resource '"
                  + context.getResourceId()
                  + "' because it is not held by holder '"
                  + command.holderId()
                  + "'");
        };
    Result<Void> result =
        this.newCommandExecutor(client).withoutTxn().retryOnCode(WRITE_CONFLICT).execute(cmdBlock);
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
