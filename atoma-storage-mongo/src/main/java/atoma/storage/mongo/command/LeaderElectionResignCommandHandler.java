package atoma.storage.mongo.command;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandlerContext;
import atoma.api.coordination.command.HandlesCommand;
import atoma.api.coordination.command.LeaderElectionCommand;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.function.Function;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

@HandlesCommand(LeaderElectionCommand.Resign.class)
public class LeaderElectionResignCommandHandler
    extends MongoCommandHandler<LeaderElectionCommand.Resign, Void> {

  @Override
  public Void execute(LeaderElectionCommand.Resign command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection =
        client.getDatabase("atoma_db").getCollection("elections");

    Function<ClientSession, Void> cmdBlock =
        session -> {
          // Atomically delete the document, but only if we are still the leader.
          collection.deleteOne(
              session, and(eq("_id", context.getResourceId()), eq("leader_id", command.leaseId())));
          return null;
        };

    try {
      return this.newExecution(client).execute(cmdBlock).getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
