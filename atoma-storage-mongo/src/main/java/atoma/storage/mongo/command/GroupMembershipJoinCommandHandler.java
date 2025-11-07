package atoma.storage.mongo.command;

import atoma.api.AtomaStateException;
import atoma.api.coordination.command.GroupMembershipCommand;
import atoma.api.coordination.command.HandlesCommand;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

import java.util.function.Function;

import static atoma.storage.mongo.command.MongoErrorCode.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

@HandlesCommand(GroupMembershipCommand.Join.class)
public class GroupMembershipJoinCommandHandler
    extends MongoCommandHandler<GroupMembershipCommand.Join, Void> {

  @Override
  public Void execute(GroupMembershipCommand.Join command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = client.getDatabase("atoma_db").getCollection("groups");

    Function<ClientSession, Void> cmdBlock =
        session -> {
          collection.updateOne(
              session,
              and(eq("_id", context.getResourceId()), ne("members.id", command.leaseId())),
              combine(
                  addToSet(
                      "members",
                      new Document("id", command.leaseId()).append("metadata", command.metadata())),
                  inc("version", 1L),
                  setOnInsert("version", 1L)),
              new UpdateOptions().upsert(true));
          return null;
        };

    try {
      return this.newExecution(client).retryOnCode(WRITE_CONFLICT).execute(cmdBlock).getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
