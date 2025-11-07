package atoma.storage.mongo.command;

import atoma.api.coordination.Resource;
import atoma.api.coordination.command.CommandHandlerContext;
import com.mongodb.client.MongoClient;

import java.util.Optional;

public class MongoCommandHandlerContext implements CommandHandlerContext {

  private final MongoClient client;
  private final String dbName;
  private final String resourceId;

  public MongoCommandHandlerContext(MongoClient client, String dbName, String resourceId) {
    this.client = client;
    this.dbName = dbName;
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

  public String getDbName() {
    return dbName;
  }
}