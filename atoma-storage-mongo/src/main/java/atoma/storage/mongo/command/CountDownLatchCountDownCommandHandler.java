package atoma.storage.mongo.command;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandlerContext;
import atoma.api.coordination.command.CountDownLatchCommand;
import atoma.api.coordination.command.HandlesCommand;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.function.Function;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Updates.inc;

/**
 * Handles the {@code countDown} operation for a distributed {@code CountDownLatch}.
 *
 * <p>This handler uses a single, atomic {@code updateOne} operation with the {@code $inc} operator
 * to decrement the {@code count} field. A crucial part of the query filter is the {@code
 * gt("count", 0)} condition. This ensures that the count never drops below zero, and that calling
 * {@code countDown()} on a completed latch (where count is 0) becomes a safe, silent no-op, which
 * is consistent with the behavior of {@link java.util.concurrent.CountDownLatch}.
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
@HandlesCommand(CountDownLatchCommand.CountDown.class)
public class CountDownLatchCountDownCommandHandler
    extends MongoCommandHandler<CountDownLatchCommand.CountDown, Void> {

  /**
   * Executes the atomic decrement command.
   *
   * @param command The {@link CountDownLatchCommand.CountDown} command.
   * @param context The context for command execution.
   * @return Void on success.
   * @throws AtomaStateException for any database or execution-related failures.
   */
  @Override
  public Void execute(CountDownLatchCommand.CountDown command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection =
        client.getDatabase("atoma_db").getCollection("countdown_latches");

    Function<ClientSession, Void> cmdBlock =
        session -> {
          collection.updateOne(
              session,
              and(
                  eq("_id", context.getResourceId()),
                  gt("count", 0) // Only decrement if count is positive
                  ),
              inc("count", -1));
          return null;
        };

    Result<Void> result = this.newExecution(client).execute(cmdBlock);
    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
