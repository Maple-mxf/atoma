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

import static com.mongodb.client.model.Filters.eq;

/** Handles fetching the current state of a distributed {@code CyclicBarrier}. */
@SuppressWarnings("rawtypes")
@AutoService({CommandHandler.class})
@HandlesCommand(CyclicBarrierCommand.GetState.class)
public class GetStateCommandHandler
    extends MongoCommandHandler<
        CyclicBarrierCommand.GetState, CyclicBarrierCommand.GetStateResult> {

  /**
   * Executes the command to fetch the current state of the barrier.
   *
   * @param command The {@link CyclicBarrierCommand.GetState} command.
   * @param context The context for command execution.
   * @return A {@link CyclicBarrierCommand.GetStateResult} containing the barrier's current state.
   * @throws AtomaStateException for any database or execution-related failures.
   */
  @Override
  public CyclicBarrierCommand.GetStateResult execute(
      CyclicBarrierCommand.GetState command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = getCollection(context, AtomaCollectionNamespace.BARRIER_NAMESPACE);

    Function<ClientSession, CyclicBarrierCommand.GetStateResult> cmdBlock =
        session -> {
          Document doc = collection.find(eq("_id", context.getResourceId())).first();
          if (doc == null) {
            return new CyclicBarrierCommand.GetStateResult(0, 0, false, 0L, 0L);
          }
          int parties = doc.getInteger("parties", 0);
          boolean isBroken = doc.getBoolean("is_broken", false);
          long generation = doc.getLong("generation");
          long version = doc.getLong("version");
          Document waiters = doc.get("waiters", Document.class);
          int numberWaiting = (waiters != null) ? waiters.getInteger("count", 0) : 0;
          return new CyclicBarrierCommand.GetStateResult(
              parties, numberWaiting, isBroken, generation, version);
        };

    Result<CyclicBarrierCommand.GetStateResult> result =
        this.newCommandExecutor(client).withoutTxn().execute(cmdBlock);
    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
