package atoma.api.coordination;

import java.util.Map;

public interface Resource {
  long getVersion();

  String getId();

  Map<String, Object> getData();

  <T> T get(String key);
}
