package atoma.storage.mongo.command.barrier;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.CyclicBarrierCommand;
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

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.unset;

/**
 * Handles the reset operation for a distributed {@code CyclicBarrier}.
 *
 * <p>This command "breaks" the current generation of the barrier (if any threads are waiting) by
 * incrementing the generation ID and setting an {@code is_broken} flag. It uses an optimistic lock
 * on the {@code version} field to ensure the reset is applied to the expected state, preventing
 * race conditions.
 */
@SuppressWarnings("rawtypes")
@HandlesCommand(CyclicBarrierCommand.Reset.class)
@AutoService({CommandHandler.class})
public class ResetCommandHandler extends MongoCommandHandler<CyclicBarrierCommand.Reset, Void> {

  /**
   * Executes the atomic reset command.
   *
   * @param command The command containing the expected version for optimistic locking.
   * @param context The context for command execution.
   * @return Void on success.
   * @throws AtomaStateException for any database or execution-related failures.
   */
  @Override
  public Void execute(CyclicBarrierCommand.Reset command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection =
        getCollection(context, AtomaCollectionNamespace.BARRIER_NAMESPACE);

    Function<ClientSession, Void> cmdBlock =
        session -> {
          collection.updateOne(
              and(
                  eq("_id", context.getResourceId()),
                  eq("version", command.expectedVersion()) // Optimistic lock
                  ),
              combine(
                  inc("generation", 1L), // Break current waiters
                  inc("version", 1L),
                  set("is_broken", true), // Mark as broken
                  unset("waiters") // Clear all waiters
                  ));
          return null;
        };

    Result<Void> result = this.newCommandExecutor(client).withoutTxn().execute(cmdBlock);
    try {
      result.getOrThrow();
      return null;
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
