package atoma.storage.mongo.command;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandlerContext;
import atoma.api.coordination.command.HandlesCommand;
import atoma.api.coordination.command.SemaphoreCommand;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

import java.util.function.Function;

import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Updates.*;

/**
 * Handles the server-side logic for acquiring permits from a distributed semaphore.
 *
 * <p>The logic is designed to be atomic and handles two main scenarios within a single command
 * execution:
 *
 * <ol>
 *   <li><b>Acquisition from Existing Semaphore:</b> It first attempts a {@code findOneAndUpdate}
 *       operation, conditioned on {@code available_permits} being sufficient. If this succeeds, the
 *       permits are acquired atomically.
 *   <li><b>Semaphore Creation (Upsert):</b> If the first attempt fails (e.g., the document does not
 *       exist or has insufficient permits), it then attempts an {@code updateOne} with {@code
 *       upsert=true}. This operation will create and initialize the semaphore only if it doesn't
 *       exist and has enough permits from the start.
 * </ol>
 *
 * The handler returns a result indicating if the acquisition was successful in either case.
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
 *
 * <ul>
 *   <li><b>_id</b>: The unique ID of the semaphore resource.
 *   <li><b>initial_permits</b>: The total number of permits defined upon creation.
 *   <li><b>available_permits</b>: The current number of available permits.
 *   <li><b>leases</b>: A map tracking the number of permits held by each client lease, crucial for
 *       safe releases and automatic cleanup on lease expiration.
 * </ul>
 */
@HandlesCommand(SemaphoreCommand.Acquire.class)
public class SemaphoreAcquireCommandHandler
    extends MongoCommandHandler<SemaphoreCommand.Acquire, SemaphoreCommand.AcquireResult> {

  /**
   * Executes the atomic logic to acquire permits from the semaphore.
   *
   * @param command The {@link SemaphoreCommand.Acquire} command, containing the number of permits
   *     requested and caller identification.
   * @param context The context for command execution.
   * @return A {@link SemaphoreCommand.AcquireResult} indicating whether the acquisition was
   *     successful.
   * @throws AtomaStateException for any database or execution-related failures.
   */
  @Override
  public SemaphoreCommand.AcquireResult execute(
      SemaphoreCommand.Acquire command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection =
        client.getDatabase("atoma_db").getCollection("semaphores");
    final String leaseField = "leases." + command.leaseId();

    Function<ClientSession, SemaphoreCommand.AcquireResult> cmdBlock =
        session -> {
          // First, try to acquire from an existing semaphore.
          // Condition: available_permits must be sufficient.
          Document updatedDoc =
              collection.findOneAndUpdate(
                  session,
                  and(
                      eq("_id", context.getResourceId()),
                      gte("available_permits", command.permits())),
                  combine(
                      inc("available_permits", -command.permits()),
                      inc(leaseField, command.permits())),
                  new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

          if (updatedDoc != null) {
            return new SemaphoreCommand.AcquireResult(true);
          }

          // If the above failed, it means either the semaphore doesn't exist or has insufficient
          // permits.
          // We now try to create it, but only if it does not exist.
          long modifiedCount =
              collection
                  .updateOne(
                      session,
                      and(
                          eq("_id", context.getResourceId()),
                          gte(
                              "available_permits",
                              command.permits()) // Re-check condition to avoid race
                          ),
                      combine(
                          setOnInsert("initial_permits", command.initialPermits()),
                          setOnInsert(
                              "available_permits", command.initialPermits() - command.permits()),
                          setOnInsert(leaseField, command.permits())),
                      new UpdateOptions().upsert(true))
                  .getModifiedCount();

          if (modifiedCount > 0) {
            return new SemaphoreCommand.AcquireResult(true);
          }

          // If we reach here, all attempts failed.
          return new SemaphoreCommand.AcquireResult(false);
        };

    ExecutionBuilder<SemaphoreCommand.AcquireResult> execution =
        this.newExecution(client).retryOnCode(WRITE_CONFLICT);

    if (command.timeout() > 0 && command.timeUnit() != null) {
      execution.withTimeout(
          java.time.Duration.of(command.timeout(), command.timeUnit().toChronoUnit()));
    }

    Result<SemaphoreCommand.AcquireResult> result = execution.execute(cmdBlock);

    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
