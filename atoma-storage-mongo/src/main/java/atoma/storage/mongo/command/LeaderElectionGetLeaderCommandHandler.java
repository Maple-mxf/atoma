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

import java.util.Optional;
import java.util.function.Function;

import static com.mongodb.client.model.Filters.eq;

@HandlesCommand(LeaderElectionCommand.GetLeader.class)
public class LeaderElectionGetLeaderCommandHandler
    extends MongoCommandHandler<LeaderElectionCommand.GetLeader, LeaderElectionCommand.GetLeaderResult> {

  @Override
  public LeaderElectionCommand.GetLeaderResult execute(
      LeaderElectionCommand.GetLeader command, CommandHandlerContext ctx) {
    MongoCommandHandlerContext context = (MongoCommandHandlerContext) ctx;
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = client.getDatabase("atoma_db").getCollection("elections");

    Function<ClientSession, LeaderElectionCommand.GetLeaderResult> cmdBlock =
        session -> {
          Document doc = collection.find(session, eq("_id", context.getResourceId())).first();
          return new LeaderElectionCommand.GetLeaderResult(Optional.ofNullable(doc).map(d -> d.getString("leader_id")));
        };

    try {
      return this.newExecution(client).execute(cmdBlock).getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
