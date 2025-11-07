package atoma.storage.mongo.command;

import atoma.api.AtomaStateException;
import atoma.api.coordination.command.CommandHandlerContext;
import atoma.api.coordination.command.DoubleCyclicBarrierCommand;
import atoma.api.coordination.command.HandlesCommand;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;

import java.util.List;
import java.util.function.Function;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

/**
 * Handles the server-side logic for the {@code enter} phase of a distributed {@link
 * atoma.api.synchronizer.DoubleCyclicBarrier}.
 *
 * <h3>Core Logic & Concurrency Control</h3>
 *
 * <p>This handler uses an optimistic locking strategy to manage concurrent arrivals at the barrier.
 * The entire operation is designed to be retryable by the client in case of a concurrency conflict.
 *
 * <ol>
 *   <li><b>Get-or-Create State:</b> It first executes an atomic {@code findOneAndUpdate} with
 *       {@code upsert=true}. This fetches the current state of the barrier document or creates it
 *       if it's the first-ever participant. This initial read also retrieves the document's current
 *       {@code version} number.
 *   <li><b>Optimistic Locking:</b> This {@code version} is then used in the filter of the
 *       subsequent write operation. If the document was modified by another process after the
 *       initial read, the version number will not match, the update will have no effect (modified
 *       count will be 0), and the command will return a "not passed" result, signaling to the
 *       client that a retry is necessary.
 *   <li><b>State-Driven Action:</b> Based on the state of the {@code enter_waiters} sub-document
 *       for the current generation, one of three atomic actions is taken:
 *       <ul>
 *         <li><b>First Participant:</b> If no one is waiting for the current generation, it
 *             initializes the {@code enter_waiters} sub-document, setting the count to 1 and adding
 *             the participant.
 *         <li><b>Last Participant:</b> If this is the last required participant, it "trips" the
 *             enter barrier by unsetting the entire {@code enter_waiters} sub-document. This
 *             deletion serves as the signal for waiting clients.
 *         <li><b>Intermediate Participant:</b> Otherwise, it increments the waiter count and adds
 *             the new participant to the list, ensuring not to add the same participant twice.
 *       </ul>
 * </ol>
 *
 * <h3>MongoDB Document Schema</h3>
 *
 * <pre>{@code
 * {
 *   "_id": "barrier-resource-id",
 *   "parties": 5,
 *   "generation": 1,
 *   "version": 12,
 *   "enter_waiters": {
 *     "generation": 1,
 *     "count": 2,
 *     "participants": [
 *       { "participant": "lease-abc___thread-1", "lease": "lease-abc" },
 *       { "participant": "lease-xyz___thread-8", "lease": "lease-xyz" }
 *     ]
 *   },
 *   "leave_waiters": { ... }
 * }
 * }</pre>
 */
@HandlesCommand(DoubleCyclicBarrierCommand.Enter.class)
public class DoubleCyclicBarrierEnterCommandHandler
    extends MongoCommandHandler<
        DoubleCyclicBarrierCommand.Enter, DoubleCyclicBarrierCommand.EnterResult> {

  /**
   * Executes the atomic logic for a participant to enter the barrier.
   *
   * @param command The command containing the participant's identity and barrier properties.
   * @param context The context for command execution, including the resource ID.
   * @return A {@link DoubleCyclicBarrierCommand.EnterResult} where {@code passed()} is {@code true}
   *     if the database state was successfully modified, and {@code false} if an optimistic locking
   *     conflict occurred, indicating the client should retry the command.
   */
  @Override
  public DoubleCyclicBarrierCommand.EnterResult execute(
      DoubleCyclicBarrierCommand.Enter command, CommandHandlerContext context) {
    MongoCommandHandlerContext ctx = (MongoCommandHandlerContext) context;
    MongoClient client = ctx.getClient();
    MongoCollection<Document> collection =
        client.getDatabase("atoma_db").getCollection("double_barriers");

    Function<ClientSession, DoubleCyclicBarrierCommand.EnterResult> cmdBlock =
        session -> {
          Document barrier =
              collection.findOneAndUpdate(
                  session,
                  eq("_id", ctx.getResourceId()),
                  combine(
                      setOnInsert("parties", command.parties()),
                      setOnInsert("generation", 0L),
                      setOnInsert("version", 1L)),
                  new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

          if (barrier == null) throw new AtomaStateException("Failed to find or create barrier.");

          long readVersion = barrier.getLong("version");
          long currentGeneration = barrier.getLong("generation");
          Document waiters = barrier.get("enter_waiters", Document.class);

          boolean isFirst = (waiters == null || waiters.getLong("generation") != currentGeneration);

          if (isFirst) {
            long count =
                collection
                    .updateOne(
                        session,
                        and(eq("_id", ctx.getResourceId()), eq("version", readVersion)),
                        combine(
                            set(
                                "enter_waiters",
                                new Document("generation", currentGeneration)
                                    .append("count", 1)
                                    .append(
                                        "participants",
                                        List.of(
                                            new Document("participant", command.participantId())
                                                .append("lease", command.leaseId())))),
                            inc("version", 1L)))
                    .getModifiedCount();
            return new DoubleCyclicBarrierCommand.EnterResult(count > 0);
          } else {
            // Idempotency Check: If participant already exists, return success immediately.
            List<Document> participants = waiters.getList("participants", Document.class, List.of());
            boolean alreadyExists = participants.stream()
                .anyMatch(p -> command.participantId().equals(p.getString("participant")));

            if (alreadyExists) {
              return new DoubleCyclicBarrierCommand.EnterResult(true);
            }

            int currentWaiters = waiters.getInteger("count", 0);
            if (currentWaiters == command.parties() - 1) {
              long count =
                  collection
                      .updateOne(
                          session,
                          and(eq("_id", ctx.getResourceId()), eq("version", readVersion)),
                          combine(unset("enter_waiters"), inc("version", 1L)))
                      .getModifiedCount();
              return new DoubleCyclicBarrierCommand.EnterResult(count > 0);
            } else {
              long count =
                  collection
                      .updateOne(
                          session,
                          // The idempotency check is now handled above, so we only need to check version.
                          and(eq("_id", ctx.getResourceId()), eq("version", readVersion)),
                          combine(
                              inc("enter_waiters.count", 1),
                              inc("version", 1L),
                              push(
                                  "enter_waiters.participants",
                                  new Document("participant", command.participantId())
                                      .append("lease", command.leaseId()))))
                      .getModifiedCount();
              return new DoubleCyclicBarrierCommand.EnterResult(count > 0);
            }
          }
        };

    try {
      return this.newExecution(client).execute(cmdBlock).getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
