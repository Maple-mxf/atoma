package atoma.storage.mongo.command.semaphore;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.HandlesCommand;
import atoma.api.coordination.command.SemaphoreCommand;
import atoma.storage.mongo.command.MongoCommandHandler;
import atoma.storage.mongo.command.MongoCommandHandlerContext;
import com.google.auto.service.AutoService;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.Optional;
import java.util.function.Function;

import static atoma.storage.mongo.command.AtomaCollectionNamespace.SEMAPHORE_NAMESPACE;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

/**
 * Handles the command to retrieve the current state of a distributed semaphore from MongoDB.
 *
 * <p>This handler fetches the total number of available permits and the number of permits currently
 * held by the specific lease associated with the command. If the semaphore document does not yet
 * exist in the database, it returns a default state where available permits are equal to the
 * initial permits specified in the command, and the permits held by the lease are zero.
 *
 * <h3>MongoDB Document Schema for Semaphore</h3>
 *
 * <pre>{@code
 * {
 *   "_id": "semaphore-resource-id",
 *   "initial_permits": 10,
 *   "available_permits": 4,
 *   "leases": {
 *     "lease-abc": 3,
 *     "lease-xyz": 3
 *   },
 *   "version": NumberLong(1),
 *   "_update_flag:": true
 * }
 * </pre>
 *
 * @see SemaphoreCommand.GetState
 * @see SemaphoreCommand.GetStateResult
 */
@AutoService({CommandHandler.class})
@HandlesCommand(SemaphoreCommand.GetState.class)
public class GetStateCommandHandler
    extends MongoCommandHandler<SemaphoreCommand.GetState, SemaphoreCommand.GetStateResult> {

  /**
   * Executes the logic to fetch the state of the semaphore from the MongoDB database.
   *
   * <p>The query projects only the necessary fields for efficiency: {@code available_permits} and
   * the number of permits held by the specific lease ID from the command (which is aliased to
   * {@code drain_permits}).
   *
   * <p>If the semaphore document is not found, it gracefully defaults to a state representing an
   * uninitialized semaphore, using the initial permit count from the command. The operation is
   * executed with retry logic for transient write conflicts.
   *
   * @param command The {@link SemaphoreCommand.GetState} command, containing the resource ID and
   *     the lease ID for which to check the state.
   * @param context The context for command execution, providing access to the {@link MongoClient}.
   * @return A {@link SemaphoreCommand.GetStateResult} containing the number of available permits
   *     and the permits held by the specific lease.
   * @throws AtomaStateException if a non-recoverable database error occurs during execution.
   */
  @Override
  protected SemaphoreCommand.GetStateResult execute(
      SemaphoreCommand.GetState command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = getCollection(context, SEMAPHORE_NAMESPACE);
    final String leaseField = "leases." + command.leaseId();

    Function<ClientSession, SemaphoreCommand.GetStateResult> cmdBlock =
        session -> {
          Document semaphoreDoc =
              collection
                  .find(eq("_id", context.getResourceId()))
                  .projection(
                      fields(
                          include("available_permits"),
                          computed("drain_permits", "$" + leaseField)))
                  .first();
          return Optional.ofNullable(semaphoreDoc)
              .map(
                  sem ->
                      new SemaphoreCommand.GetStateResult(
                          sem.getInteger("available_permits"), sem.getInteger("drain_permits", 0)))
              .orElse(new SemaphoreCommand.GetStateResult(command.initialPermits(), 0));
        };

    Result<SemaphoreCommand.GetStateResult> result =
        this.newCommandExecutor(client).withoutTxn().withoutCausallyConsistent().execute(cmdBlock);

    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      if (e instanceof IllegalStateException) {
        throw (IllegalStateException) e;
      }
      throw new AtomaStateException(e);
    }
  }
}
