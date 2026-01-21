package atoma.storage.mongo.command.lease;

import atoma.api.AtomaStateException;
import atoma.api.coordination.command.CommandHandler;
import atoma.api.coordination.command.HandlesCommand;
import atoma.api.coordination.command.LeaseCommand;
import atoma.storage.mongo.command.AtomaCollectionNamespace;
import atoma.storage.mongo.command.MongoCommandHandler;
import atoma.storage.mongo.command.MongoCommandHandlerContext;
import com.google.auto.service.AutoService;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;

import java.util.function.Function;

import static com.mongodb.client.model.Filters.eq;

@SuppressWarnings("rawtypes")
@AutoService({CommandHandler.class})
@HandlesCommand(LeaseCommand.Revoke.class)
public class RevokeCommandHandler
    extends MongoCommandHandler<LeaseCommand.Revoke, LeaseCommand.RevokeResult> {
  @Override
  protected LeaseCommand.RevokeResult execute(
      LeaseCommand.Revoke command, MongoCommandHandlerContext context) {
    MongoCollection<Document> collection =
        getCollection(context, AtomaCollectionNamespace.LEASE_NAMESPACE);

    String leaseId = command.id();

    Function<ClientSession, LeaseCommand.RevokeResult> cmdBlock =
        session -> {
          // Execute the delete operation
          DeleteResult deleteResult = collection.deleteOne(eq("_id", leaseId));
          return new LeaseCommand.RevokeResult(deleteResult.wasAcknowledged());
        };

    try {
      return this.newCommandExecutor(context.getClient()).withoutTxn().execute(cmdBlock).getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
