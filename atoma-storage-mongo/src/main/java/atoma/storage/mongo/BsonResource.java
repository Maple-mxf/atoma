package atoma.storage.mongo;

import atoma.api.coordination.Resource;
import org.bson.Document;

import java.util.Map;

public final class BsonResource implements Resource {

  private final Document bson;

  public BsonResource(Document bson) {
    this.bson = bson;
  }

  @Override
  public long getVersion() {
    return this.bson.getLong("version");
  }

  @Override
  public String getId() {
    return this.bson.getString("_id");
  }

  @Override
  public Map<String, Object> getData() {
    return this.bson;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T get(String key) {
    return (T) this.bson.get(key);
  }
}
