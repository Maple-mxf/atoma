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
 * Handles the explicit deletion of a {@code CountDownLatch} resource.
 *
 * <p>This handler performs a simple {@code deleteOne} operation to permanently remove the latch
 * document from the database. This allows for manual resource cleanup by the user.
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
@HandlesCommand(CountDownLatchCommand.Destroy.class)
@AutoService({CommandHandler.class})
public class DestroyCommandHandler
    extends MongoCommandHandler<CountDownLatchCommand.Destroy, Void> {

  /**
   * Executes the atomic deletion command.
   *
   * @param command The {@link CountDownLatchCommand.Destroy} command.
   * @param context The context for command execution.
   * @return Void on success.
   * @throws AtomaStateException for any database or execution-related failures.
   */
  @Override
  public Void execute(CountDownLatchCommand.Destroy command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection =
            getCollection(context, AtomaCollectionNamespace.COUNTDOWN_LATCH_NAMESPACE);

    Function<ClientSession, Void> cmdBlock =
        session -> {
          collection.deleteOne(eq("_id", context.getResourceId()));
          return null;
        };

    Result<Void> result = this.newCommandExecutor(client).withoutTxn().execute(cmdBlock);
    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
