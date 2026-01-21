package atoma.storage.mongo.command.cdl;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.CountDownLatchCommand;
import atoma.api.coordination.command.HandlesCommand;
import atoma.storage.mongo.command.AtomaCollectionNamespace;
import atoma.storage.mongo.command.MongoCommandHandler;
import atoma.storage.mongo.command.MongoCommandHandlerContext;
import com.google.auto.service.AutoService;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.function.Function;

import static com.mongodb.client.model.Filters.eq;

/**
 * Handles fetching the current count of a distributed {@code CountDownLatch}.
 *
 * <p>This handler performs a {@code find} operation to retrieve the latch document. If the document
 * does not exist (e.g., it was never created or has been destroyed), it correctly returns a count
 * of 0, as any {@code await()} call on such a latch should pass immediately.
 *
 * <h3>MongoDB Document Schema for CountDownLatch</h3>
 *
 * <pre>{@code
 * {
 *   "_id": "latch-resource-id",
 *   "count": 3
 * }
 * }</pre>
 */
@SuppressWarnings("rawtypes")
@HandlesCommand(CountDownLatchCommand.GetCount.class)
@AutoService({CommandHandler.class})
public class GetCountCommandHandler
    extends MongoCommandHandler<
            CountDownLatchCommand.GetCount, CountDownLatchCommand.GetCountResult> {

  /**
   * Executes the command to fetch the current count.
   *
   * @param command The {@link CountDownLatchCommand.GetCount} command.
   * @param context The context for command execution.
   * @return A {@link CountDownLatchCommand.GetCountResult} containing the current count.
   * @throws AtomaStateException for any database or execution-related failures.
   */
  @Override
  public CountDownLatchCommand.GetCountResult execute(
      CountDownLatchCommand.GetCount command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
      MongoCollection<Document> collection =
              getCollection(context, AtomaCollectionNamespace.COUNTDOWN_LATCH_NAMESPACE);

    Function<ClientSession, CountDownLatchCommand.GetCountResult> cmdBlock =
        session -> {
          Document doc = collection.find(eq("_id", context.getResourceId())).first();
          if (doc != null && doc.getInteger("count") != null) {
            return new CountDownLatchCommand.GetCountResult(doc.getInteger("count"));
          } else {
            // If doc doesn't exist, count is effectively 0 for any waiting threads.
            return new CountDownLatchCommand.GetCountResult(0);
          }
        };

    Result<CountDownLatchCommand.GetCountResult> result =
        this.newCommandExecutor(client).withoutTxn().execute(cmdBlock);
    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
