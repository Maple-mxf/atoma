package atoma.storage.mongo.command.cdl;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.CountDownLatchCommand;
import atoma.api.coordination.command.HandlesCommand;
import atoma.storage.mongo.command.MongoCommandHandler;
import atoma.storage.mongo.command.MongoCommandHandlerContext;
import com.google.auto.service.AutoService;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

import java.util.function.Function;

import static atoma.storage.mongo.command.AtomaCollectionNamespace.COUNTDOWN_LATCH_NAMESPACE;
import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.setOnInsert;

/**
 * Handles the one-time initialization of a distributed {@code CountDownLatch}.
 *
 * <p>This handler uses an {@code updateOne} operation with {@code upsert=true} and the {@code
 * $setOnInsert} operator. This ensures that the latch's count is set only when the document is
 * first created. If the latch document already exists, this operation has no effect, thus
 * preventing the count of an existing latch from being accidentally reset.
 *
 * <h3>MongoDB Document Schema for CountDownLatch</h3>
 *
 * <pre>{@code
 * {
 *   "_id": "latch-resource-id",
 *   "count": 3,
 *   "_update_flag:": true
 * }
 * }</pre>
 */
@SuppressWarnings("rawtypes")
@HandlesCommand(CountDownLatchCommand.Initialize.class)
@AutoService({CommandHandler.class})
public class InitializeCommandHandler
    extends MongoCommandHandler<CountDownLatchCommand.Initialize, Void> {

  /**
   * Executes the atomic initialization command.
   *
   * @param command The command containing the initial count.
   * @param context The context for command execution.
   * @return Void on success.
   * @throws AtomaStateException for any database or execution-related failures.
   */
  @Override
  public Void execute(
      CountDownLatchCommand.Initialize command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = getCollection(context, COUNTDOWN_LATCH_NAMESPACE);

    Function<ClientSession, Void> cmdBlock =
        session -> {
          collection.updateOne(
              eq("_id", context.getResourceId()),
              setOnInsert("count", command.count()),
              new UpdateOptions().upsert(true));
          return null;
        };

    Result<Void> result =
        this.newCommandExecutor(client)
            .withoutTxn()
            .withoutCausallyConsistent()
            .retryOnCode(WRITE_CONFLICT)
            .execute(cmdBlock);
    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
