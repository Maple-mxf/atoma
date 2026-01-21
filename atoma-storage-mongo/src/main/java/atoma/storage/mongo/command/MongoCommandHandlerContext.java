package atoma.storage.mongo.command;

import atoma.api.coordination.Resource;
import atoma.api.coordination.command.CommandHandlerContext;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

import java.util.Optional;

public class MongoCommandHandlerContext implements CommandHandlerContext {
  private final MongoClient client;
  private final String resourceId;
  private final MongoDatabase mongoDatabase;

  public MongoCommandHandlerContext(
      MongoClient client, MongoDatabase mongoDatabase, String resourceId) {
    this.client = client;
    this.mongoDatabase = mongoDatabase;
    this.resourceId = resourceId;
  }

  @Override
  public String getResourceId() {
    return resourceId;
  }

  @Override
  public Optional<Resource> getCurrentResource() {
    // This could be implemented to pre-fetch the resource if needed.
    return Optional.empty();
  }

  public MongoClient getClient() {
    return client;
  }

  public MongoDatabase getMongoDatabase() {
    return mongoDatabase;
  }
}
