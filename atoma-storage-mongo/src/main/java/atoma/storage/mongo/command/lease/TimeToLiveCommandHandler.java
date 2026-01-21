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
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;

import java.time.Instant;
import java.util.Date;
import java.util.function.Function;

@SuppressWarnings("rawtypes")
@AutoService(CommandHandler.class)
@HandlesCommand(LeaseCommand.TimeToLive.class)
public class TimeToLiveCommandHandler
    extends MongoCommandHandler<LeaseCommand.TimeToLive, LeaseCommand.TimeToLiveResult> {
  @Override
  protected LeaseCommand.TimeToLiveResult execute(
      LeaseCommand.TimeToLive command, MongoCommandHandlerContext context) {

    MongoCollection<Document> collection =
        getCollection(context, AtomaCollectionNamespace.LEASE_NAMESPACE);

    Function<ClientSession, LeaseCommand.TimeToLiveResult> cmdBlock =
        (session) -> {
          // Query to find an existing lease that is either expired or does not exist
          Document query = new Document("_id", command.id());

          // Updates to apply
          Document update =
              new Document("$set", new Document("expire_time", command.nextExpireTime()))
                  .append("$inc", new Document("version", 1L));

          FindOneAndUpdateOptions options =
              new FindOneAndUpdateOptions()
                  .upsert(false) // Insert a new document if no document matches the query
                  .returnDocument(ReturnDocument.AFTER); // Return the document after update

          Document updatedDocument = collection.findOneAndUpdate(query, update, options);

          if (updatedDocument != null) {
            String id = updatedDocument.getString("_id");
            long expireTimeMillis = updatedDocument.get("expire_time", Date.class).getTime();

            return new LeaseCommand.TimeToLiveResult(true, Instant.ofEpochMilli(expireTimeMillis));
          } else {
            // This should ideally not happen with upsert(true), but handle it just in case
            // For now, returning null implies failure to grant. Depending on business logic,
            // this might need to throw an exception or return a specific error result.
            return new LeaseCommand.TimeToLiveResult(false, null);
          }
        };

    try {
      return this.newCommandExecutor(context.getClient())
          .withoutTxn()
          .execute(cmdBlock)
          .getOrThrow();
    } catch (Throwable e) {
      throw new AtomaStateException(e);
    }
  }
}
