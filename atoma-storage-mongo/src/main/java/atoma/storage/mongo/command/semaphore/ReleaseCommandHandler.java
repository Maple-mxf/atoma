package atoma.storage.mongo.command.semaphore;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.HandlesCommand;
import atoma.api.coordination.command.SemaphoreCommand;
import atoma.storage.mongo.command.AtomaCollectionNamespace;
import atoma.storage.mongo.command.MongoCommandHandler;
import atoma.storage.mongo.command.MongoCommandHandlerContext;
import com.google.auto.service.AutoService;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import org.bson.Document;

import java.util.function.Function;

import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;

/**
 * Handles the server-side logic for releasing permits to a distributed semaphore.
 *
 * <p>The operation is performed via a single, atomic {@code findOneAndUpdate} command. It
 * increments the global {@code available_permits} count while decrementing the number of permits
 * held by the specific client lease, which is tracked in the {@code leases} map.
 *
 * <p><b>Safety Check:</b> A critical condition in the update query ({@code gte(leaseField,
 * permits)}) ensures that a client cannot release more permits than it currently holds, preventing
 * corruption of the semaphore's state. If this condition fails, the operation finds no document to
 * update, and the handler throws an {@link IllegalStateException} to signal a critical client-side
 * error.
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
 *   }
 * }
 * }</pre>
 */
@SuppressWarnings("rawtypes")
@AutoService({CommandHandler.class})
@HandlesCommand(SemaphoreCommand.Release.class)
public class ReleaseCommandHandler extends MongoCommandHandler<SemaphoreCommand.Release, Void> {

  /**
   * Executes the atomic logic to release permits back to the semaphore.
   *
   * @param command The {@link SemaphoreCommand.Release} command, containing the number of permits
   *     to release and caller identification.
   * @param context The context for command execution.
   * @return Void on success.
   * @throws IllegalStateException if the release fails because the lease does not hold enough
   *     permits.
   * @throws AtomaStateException for any other database or execution-related failures.
   */
  @Override
  public Void execute(SemaphoreCommand.Release command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection =
        getCollection(context, AtomaCollectionNamespace.SEMAPHORE_NAMESPACE);
    final String leaseField = "leases." + command.leaseId();

    Function<ClientSession, Void> cmdBlock =
        session -> {
          // Atomically increase available permits and decrease the lease's held permits.
          // We add a condition to ensure a lease cannot release more permits than it holds.
          Document updatedDoc =
              collection.findOneAndUpdate(
                  and(
                      eq("_id", context.getResourceId()),
                      gte(leaseField, command.permits()) // Ensure we don't go below zero
                      ),
                  combine(
                      inc("available_permits", command.permits()),
                      inc(leaseField, -command.permits())),
                  new FindOneAndUpdateOptions().upsert(false));

          if (updatedDoc != null) {
            // Success, return nothing.
            return null;
          }

          // If the update failed, it's an illegal state. Throw an exception.
          throw new IllegalStateException(
              "Failed to release "
                  + command.permits()
                  + " permits for lease "
                  + command.leaseId()
                  + ". Either the semaphore does not exist or the lease does not hold enough permits.");
        };

    Result<Void> result =
        this.newCommandExecutor(client).withoutTxn().retryOnCode(WRITE_CONFLICT).execute(cmdBlock);

    try {
      // If successful, returns null (Void). If it failed, throws the underlying exception.
      return result.getOrThrow();
    } catch (Throwable e) {
      if (e instanceof IllegalStateException) {
        throw (IllegalStateException) e;
      }
      throw new AtomaStateException(e);
    }
  }
}
