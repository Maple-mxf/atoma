package atoma.storage.mongo.command;

import atoma.api.AtomaStateException;
import atoma.api.coordination.command.HandlesCommand;
import atoma.api.coordination.command.LeaderElectionCommand;
import com.mongodb.MongoWriteException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.function.Function;

import static com.mongodb.client.model.Filters.eq;

@HandlesCommand(LeaderElectionCommand.Campaign.class)
public class LeaderElectionCampaignCommandHandler
    extends MongoCommandHandler<
        LeaderElectionCommand.Campaign, LeaderElectionCommand.CampaignResult> {

  @Override
  public LeaderElectionCommand.CampaignResult execute(
      LeaderElectionCommand.Campaign command, MongoCommandHandlerContext context) {
    MongoClient client = context.getClient();
    MongoCollection<Document> collection =
        client.getDatabase("atoma_db").getCollection("elections");

    Function<ClientSession, LeaderElectionCommand.CampaignResult> cmdBlock =
        session -> {
          try {
            // Attempt to insert a document with the resourceId as the _id.
            // A unique index on _id ensures only one can ever exist.
            collection.insertOne(
                session,
                new Document("_id", context.getResourceId())
                    .append("leader_id", command.leaseId()));
            // If insert succeeds, we are the leader.
            return new LeaderElectionCommand.CampaignResult(true);
          } catch (MongoWriteException e) {
            if (e.getError().getCategory().equals(com.mongodb.ErrorCategory.DUPLICATE_KEY)) {
              // A duplicate key error means a leader document already exists. We are not the
              // leader.
              // We can check if WE are the leader in case of a retry.
              Document doc = collection.find(session, eq("_id", context.getResourceId())).first();
              boolean amILeader =
                  (doc != null && command.leaseId().equals(doc.getString("leader_id")));
              return new LeaderElectionCommand.CampaignResult(amILeader);
            }
            throw e;
          }
        };

    try {
      return this.newExecution(client).execute(cmdBlock).getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
