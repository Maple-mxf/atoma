package atoma.storage.mongo.command;

import atoma.api.AtomaStateException;
import atoma.api.Result;
import atoma.api.cluster.GroupMember;
import atoma.api.coordination.command.CommandHandlerContext;
import atoma.api.coordination.command.GroupMembershipCommand;
import atoma.api.coordination.command.HandlesCommand;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;

@HandlesCommand(GroupMembershipCommand.GetMembers.class)
public class GroupMembershipGetMembersCommandHandler
    extends MongoCommandHandler<GroupMembershipCommand.GetMembers, GroupMembershipCommand.GetMembersResult> {

  @Override
  public GroupMembershipCommand.GetMembersResult execute(
      GroupMembershipCommand.GetMembers command, CommandHandlerContext ctx) {
    MongoCommandHandlerContext context = (MongoCommandHandlerContext) ctx;
    MongoClient client = context.getClient();
    MongoCollection<Document> collection = client.getDatabase("atoma_db").getCollection("groups");

    Function<ClientSession, GroupMembershipCommand.GetMembersResult> cmdBlock =
        session -> {
          Document doc = collection.find(session, eq("_id", context.getResourceId())).first();
          if (doc == null || doc.getList("members", Document.class) == null) {
            return new GroupMembershipCommand.GetMembersResult(List.of());
          }
          List<GroupMember> members = doc.getList("members", Document.class).stream()
              .map(memberDoc -> (GroupMember) new DocumentGroupMember(memberDoc))
              .collect(Collectors.toList());
          return new GroupMembershipCommand.GetMembersResult(members);
        };

    try {
      return this.newExecution(client).execute(cmdBlock).getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
