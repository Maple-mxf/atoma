package atoma.storage.mongo.command;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandlerContext;
import atoma.api.coordination.command.CyclicBarrierCommand;
import atoma.api.coordination.command.HandlesCommand;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;

import java.util.List;
import java.util.function.Function;

import static atoma.storage.mongo.command.MongoErrorCode.WRITE_CONFLICT;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

/**
 * Handles the {@code await} operation for a distributed {@code CyclicBarrier}.
 *
 * <p>This implementation is highly optimized for correctness and performance under high contention.
 * It uses a combination of a "check-then-act" pattern and optimistic locking via a {@code version}
 * field.
 *
 * <h4>Core Logic</h4>
 *
 * The execution flow is as follows:
 *
 * <ol>
 *   <li><b>Get-or-Create State:</b> It first performs a single atomic {@code findOneAndUpdate} with
 *       {@code upsert=true} to get the current state of the barrier document or create it if it's
 *       the very first participant ever. This operation also retrieves the document's current
 *       {@code version}.
 *   <li><b>Optimistic Locking:</b> This {@code version} number is then used in the filter of all
 *       subsequent write operations. If the document has been modified by another process between
 *       the read and the write, the version number will have changed, causing the update to fail
 *       safely. This prevents lost updates and race conditions.
 *   <li><b>Intelligent "Check-Then-Act":</b> Based on the state read in the first step, the handler
 *       intelligently chooses one of three distinct atomic operations:
 *       <ul>
 *         <li><b>Initialize Generation:</b> If this is the first participant for a new generation,
 *             it sets the entire {@code waiters} sub-document.
 *         <li><b>Trip Barrier:</b> If this is the last participant for the current generation, it
 *             increments the global {@code generation}, unsets the {@code waiters} document, and
 *             resets the {@code is_broken} flag to {@code false}.
 *         <li><b>Join Barrier:</b> Otherwise, it increments the {@code waiters.count} and pushes
 *             itself to the {@code waiters.participants} array.
 *       </ul>
 * </ol>
 *
 * This strategy avoids unnecessary database commands and ensures atomicity in a highly concurrent
 * environment.
 *
 * <h3>MongoDB Document Schema</h3>
 *
 * <pre>{@code
 * {
 *   "_id": "barrier-resource-id",
 *   "parties": 5,
 *   "generation": 1,
 *   "is_broken": false,
 *   "version": 12,
 *   "waiters": {
 *     "generation": 1,
 *     "count": 2,
 *     "participants": [
 *       { "participant": "lease-abc___thread-1", "lease": "lease-abc" },
 *       { "participant": "lease-xyz___thread-8", "lease": "lease-xyz" }
 *     ]
 *   }
 * }
 * }</pre>
 */
@HandlesCommand(CyclicBarrierCommand.Await.class)
public class CyclicBarrierAwaitCommandHandler
    extends MongoCommandHandler<CyclicBarrierCommand.Await, CyclicBarrierCommand.AwaitResult> {

  @Override
  public CyclicBarrierCommand.AwaitResult execute(
      CyclicBarrierCommand.Await command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection =
        client.getDatabase("atoma_db").getCollection("cyclic_barriers");

    Function<ClientSession, CyclicBarrierCommand.AwaitResult> cmdBlock =
        session -> {
          // 1. Get-or-Create the barrier state, retrieving its current version.
          Document barrier =
              collection.findOneAndUpdate(
                  session,
                  eq("_id", context.getResourceId()),
                  combine(
                      setOnInsert("parties", command.parties()),
                      setOnInsert("generation", 0L),
                      setOnInsert("is_broken", false),
                      setOnInsert("version", 1L)),
                  new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

          if (barrier == null) {
            throw new AtomaStateException("Failed to find or create barrier document.");
          }
          if (barrier.getBoolean("is_broken", false)) {
            return new CyclicBarrierCommand.AwaitResult(false, true);
          }

          long readVersion = barrier.getLong("version");
          long currentGeneration = barrier.getLong("generation");
          Document waiters = barrier.get("waiters", Document.class);
          int parties = barrier.getInteger("parties");

          // 2. Decide which action to take based on the current state.
          boolean isFirstPartyOfGeneration =
              (waiters == null || waiters.getLong("generation") != currentGeneration);

          if (isFirstPartyOfGeneration) {
            // Action: Initialize the waiters for the new generation.
            long updatedCount =
                collection
                    .updateOne(
                        session,
                        and(eq("_id", context.getResourceId()), eq("version", readVersion)),
                        combine(
                            set(
                                "waiters",
                                new Document("generation", currentGeneration)
                                    .append("count", 1)
                                    .append(
                                        "participants",
                                        List.of(
                                            new Document("participant", command.participantId())
                                                .append("lease", command.leaseId())))),
                            inc("version", 1L)))
                    .getModifiedCount();
            return new CyclicBarrierCommand.AwaitResult(updatedCount > 0, false);
          } else {
            // Idempotency Check: If participant already exists, return success immediately.
            List<Document> participants = waiters.getList("participants", Document.class, List.of());
            boolean alreadyExists = participants.stream()
                .anyMatch(p -> command.participantId().equals(p.getString("participant")));

            if (alreadyExists) {
              return new CyclicBarrierCommand.AwaitResult(true, false);
            }

            int currentWaiters = waiters.getInteger("count", 0);
            if (currentWaiters == parties - 1) {
              // Action: Trip the barrier and reset the broken state.
              long trippedCount =
                  collection
                      .updateOne(
                          session,
                          and(
                              eq("_id", context.getResourceId()),
                              eq("generation", currentGeneration),
                              eq("version", readVersion)),
                          combine(
                              inc("generation", 1L),
                              inc("version", 1L),
                              set("is_broken", false),
                              unset("waiters")))
                      .getModifiedCount();
              return new CyclicBarrierCommand.AwaitResult(trippedCount > 0, false);
            } else {
              // Action: Join the waiting group.
              long joinedCount =
                  collection
                      .updateOne(
                          session,
                          and(
                              eq("_id", context.getResourceId()),
                              eq("generation", currentGeneration),
                              eq("version", readVersion)),
                          combine(
                              inc("waiters.count", 1),
                              inc("version", 1L),
                              push(
                                  "waiters.participants",
                                  new Document("participant", command.participantId())
                                      .append("lease", command.leaseId()))))
                      .getModifiedCount();
              return new CyclicBarrierCommand.AwaitResult(joinedCount > 0, false);
            }
          }
        };

    Result<CyclicBarrierCommand.AwaitResult> result =
        this.newExecution(client).retryOnCode(WRITE_CONFLICT).execute(cmdBlock);
    try {
      return result.getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
