package atoma.storage.mongo.command;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.coordination.command.CommandHandlerContext;
import atoma.api.coordination.command.GroupMembershipCommand;
import atoma.api.coordination.command.HandlesCommand;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.function.Function;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;

@HandlesCommand(GroupMembershipCommand.Leave.class)
public class GroupMembershipLeaveCommandHandler
    extends MongoCommandHandler<GroupMembershipCommand.Leave, Void> {

  @Override
  public Void execute(GroupMembershipCommand.Leave command, CommandHandlerContext ctx) {
    MongoCommandHandlerContext context = (MongoCommandHandlerContext) ctx;
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = client.getDatabase("atoma_db").getCollection("groups");

    Function<ClientSession, Void> cmdBlock =
        session -> {
          collection.updateOne(session,
              eq("_id", context.getResourceId()),
              combine(
                  pull("members", eq("id", command.leaseId())),
                  inc("version", 1L)
              ));
          return null;
        };

    try {
      return this.newExecution(client).execute(cmdBlock).getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
