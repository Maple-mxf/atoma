package atoma.api.coordination;

import atoma.api.coordination.command.Command;

import java.util.Optional;

public interface CoordinationStore extends AutoCloseable {

  /**
   * @param resourceId 资源ID
   * @return 获取指定资源节点的当前快照
   */
  Optional<Resource> get(String resourceId);

  /**
   * 订阅一个资源节点的变更事件
   *
   * @param resourceType
   *     订阅的resource的类型，mutexLock代表互斥锁资源、readWriteLock代表读写锁资源、sempahore代表信号量、countDownLatch等
   * @param resourceId 要订阅的资源键。
   * @param listener 当节点发生创建、更新或删除时被调用的监听器。
   * @return 返回一个 Subscription 对象，可用于在未来取消此次订阅。
   */
  Subscription subscribe(String resourceType, String resourceId, ResourceListener listener);

  /**
   * Executes a command against a specific resource.
   *
   * @param resourceId The unique key of the target resource.
   * @param command The command to execute.
   * @param <R> The type of the result expected from this command.
   * @return A command-specific result object.
   */
  <R> R execute(String resourceId, Command<R> command);
}