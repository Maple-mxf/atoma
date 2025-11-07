package atoma.storage.mongo.command;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandlerContext;
import atoma.api.coordination.command.CyclicBarrierCommand;
import atoma.api.coordination.command.HandlesCommand;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.function.Function;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;

/**
 * Handles the reset operation for a distributed {@code CyclicBarrier}.
 *
 * <p>This command "breaks" the current generation of the barrier (if any threads are waiting) by
 * incrementing the generation ID and setting an {@code is_broken} flag. It uses an optimistic lock
 * on the {@code version} field to ensure the reset is applied to the expected state, preventing
 * race conditions.
 */
@HandlesCommand(CyclicBarrierCommand.Reset.class)
public class CyclicBarrierResetCommandHandler
    extends MongoCommandHandler<CyclicBarrierCommand.Reset, Void> {

  /**
   * Executes the atomic reset command.
   *
   * @param command The command containing the expected version for optimistic locking.
   * @param ctx The context for command execution.
   * @return Void on success.
   * @throws AtomaStateException for any database or execution-related failures.
   */
  @Override
  public Void execute(CyclicBarrierCommand.Reset command, CommandHandlerContext ctx) {
    MongoCommandHandlerContext context = (MongoCommandHandlerContext) ctx;
    MongoClient client = context.getClient();
    MongoCollection<Document> collection =
        client.getDatabase("atoma_db").getCollection("cyclic_barriers");

    Function<ClientSession, Void> cmdBlock =
        session -> {
          collection.updateOne(
              session,
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

    Result<Void> result = this.newExecution(client).execute(cmdBlock);
    try {
      result.getOrThrow();
      return null;
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
