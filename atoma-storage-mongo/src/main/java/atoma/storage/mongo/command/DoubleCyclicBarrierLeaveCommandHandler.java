package atoma.storage.mongo.command;

import atoma.api.AtomaStateException;
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

@HandlesCommand(DoubleCyclicBarrierCommand.Leave.class)
public class DoubleCyclicBarrierLeaveCommandHandler
    extends MongoCommandHandler<
        DoubleCyclicBarrierCommand.Leave, DoubleCyclicBarrierCommand.LeaveResult> {

  @Override
  public DoubleCyclicBarrierCommand.LeaveResult execute(
      DoubleCyclicBarrierCommand.Leave command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection =
        client.getDatabase("atoma_db").getCollection("double_barriers");

    Function<ClientSession, DoubleCyclicBarrierCommand.LeaveResult> cmdBlock =
        session -> {
          Document barrier =
              collection.findOneAndUpdate(
                  session,
                  eq("_id", context.getResourceId()),
                  combine(
                      setOnInsert("parties", command.parties()),
                      setOnInsert("generation", 0L),
                      setOnInsert("version", 1L)),
                  new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));

          if (barrier == null) throw new AtomaStateException("Failed to find or create barrier.");

          long readVersion = barrier.getLong("version");
          long currentGeneration = barrier.getLong("generation");
          Document waiters = barrier.get("leave_waiters", Document.class);

          boolean isFirst = (waiters == null || waiters.getLong("generation") != currentGeneration);

          if (isFirst) {
            long count =
                collection
                    .updateOne(
                        session,
                        and(eq("_id", context.getResourceId()), eq("version", readVersion)),
                        combine(
                            set(
                                "leave_waiters",
                                new Document("generation", currentGeneration)
                                    .append("count", 1)
                                    .append(
                                        "participants",
                                        List.of(
                                            new Document("participant", command.participantId())
                                                .append("lease", command.leaseId())))),
                            inc("version", 1L)))
                    .getModifiedCount();
            return new DoubleCyclicBarrierCommand.LeaveResult(count > 0);
          } else {
            int currentWaiters = waiters.getInteger("count", 0);
            if (currentWaiters == command.parties() - 1) {
              long count =
                  collection
                      .updateOne(
                          session,
                          and(eq("_id", context.getResourceId()), eq("version", readVersion)),
                          combine(
                              unset("leave_waiters"), inc("generation", 1L), inc("version", 1L)))
                      .getModifiedCount();
              return new DoubleCyclicBarrierCommand.LeaveResult(count > 0);
            } else {
              long count =
                  collection
                      .updateOne(
                          session,
                          and(
                              eq("_id", context.getResourceId()),
                              eq("version", readVersion),
                              ne(
                                  "leave_waiters.participants.participant",
                                  command.participantId())),
                          combine(
                              inc("leave_waiters.count", 1),
                              inc("version", 1L),
                              push(
                                  "leave_waiters.participants",
                                  new Document("participant", command.participantId())
                                      .append("lease", command.leaseId()))))
                      .getModifiedCount();
              return new DoubleCyclicBarrierCommand.LeaveResult(count > 0);
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
