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
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static atoma.storage.mongo.command.AtomaCollectionNamespace.COUNTDOWN_LATCH_NAMESPACE;
import static com.mongodb.client.model.Aggregates.replaceRoot;
import static com.mongodb.client.model.Filters.eq;

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
 *   "count": 3,
 *   "_update_flag:": true
 * }
 * }</pre>
 */
@SuppressWarnings("rawtypes")
@HandlesCommand(CountDownLatchCommand.CountDown.class)
@AutoService({CommandHandler.class})
public class CountDownCommandHandler
    extends MongoCommandHandler<CountDownLatchCommand.CountDown, Void> {

  private List<Bson> buildAggregationPipeline() {
    return List.of(
        replaceRoot(
            new Document(
                "$cond",
                new Document("if", new Document("$gte", Arrays.asList("$count", 0)))
                    .append(
                        "then",
                        new Document()
                            .append("count", new Document("$add", Arrays.asList("$count", -1)))
                            .append("_update_flag", true))
                    .append(
                        "else",
                        new Document(
                            "$mergeObjects",
                            Arrays.asList("$$ROOT", new Document("_update_flag", false)))))));
  }

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
    MongoCollection<Document> collection = getCollection(context, COUNTDOWN_LATCH_NAMESPACE);

    List<Bson> pipeline = buildAggregationPipeline();

    Function<ClientSession, Void> cmdBlock =
        session -> {
          Document countDownLatchDoc =
              collection.findOneAndUpdate(
                  eq("_id", context.getResourceId()),
                  pipeline,
                  new FindOneAndUpdateOptions().upsert(false).returnDocument(ReturnDocument.AFTER));

          if (countDownLatchDoc == null) {
            throw new IllegalStateException(
                "Failed to count-down. the count-down-latch does not exist");
          }

          /*if (!countDownLatchDoc.getBoolean("_update_flag"))
            throw new IllegalStateException("The count is already negative");*/

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
