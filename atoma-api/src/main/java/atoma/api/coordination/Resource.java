package atoma.api.coordination;

import java.util.Map;

public interface Resource {

  /**
   * @return 节点的版本号或时间戳，用于乐观锁。
   */
  long getVersion();

  /**
   * @return 当前资源的ID
   */
  String getId();

  /**
   * @return 节点的原始数据，以 Map 形式提供，用于逻辑层进行解析。
   */
  Map<String, Object> getData();

  <T> T get(String key);
}
