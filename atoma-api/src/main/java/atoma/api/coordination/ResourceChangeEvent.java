package atoma.api.coordination;

import java.util.Optional;

public final class ResourceChangeEvent {

  ResourceChangeEvent(EventType type, String resourceKey, Resource newNode, Resource oldNode) {
    this.type = type;
    this.resourceKey = resourceKey;
    this.newNode = Optional.ofNullable(newNode);
    this.oldNode = Optional.ofNullable(oldNode);
  }

  public enum EventType {
    /** 节点被创建。getNewNode() 将返回被创建的节点。 */
    CREATED,
    /** 节点被更新。getNewNode() 和 getOldNode() 分别返回更新后和更新前的节点状态。 */
    UPDATED,
    /** 节点被删除。getOldNode() 将返回被删除前的最后状态。 */
    DELETED
  }

  private final EventType type;
  private final String resourceKey;

  /** 删除场景下，此字段为NULL */
  private final Optional<Resource> newNode;

  /** 创建场景下，此字段为空 */
  private final Optional<Resource> oldNode;

  public EventType getType() {
    return type;
  }

  public String getResourceKey() {
    return resourceKey;
  }

  public Optional<Resource> getNewNode() {
    return newNode;
  }

  public Optional<Resource> getOldNode() {
    return oldNode;
  }
}
