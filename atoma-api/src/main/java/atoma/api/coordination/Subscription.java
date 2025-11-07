package atoma.api.coordination;

/**
 * 代表对一个资源节点变更事件的订阅。
 *
 * <p>此对象的主要职责是管理订阅的生命周期，特别是提供取消订阅的能力。 它实现了 AutoCloseable 接口，以便可以方便地在 try-with-resources 语句块中使用。
 */
public interface Subscription extends AutoCloseable {
  /**
   * 取消此次订阅。
   *
   * <p>调用此方法后，将停止接收任何后续的节点变更通知，并释放底层相关的资源, (例如数据库的监听游标)。此操作是幂等的。
   */
  void unsubscribe();

  /**
   * 检查订阅当前是否仍处于活动状态。
   *
   * @return 如果订阅有效且未被取消，则返回 true；否则返回 false。
   */
  boolean isSubscribed();

  /**
   * 获取此订阅所关联的资源键。
   *
   * @return 资源的唯一标识键。
   */
  String getResourceKey();

  /**
   * 实现 AutoCloseable 接口，其行为等同于调用 {@link #unsubscribe()}。 这使得 Subscription 对象可以被用于
   * try-with-resources 语句块中。
   */
  default void close() {
    unsubscribe();
  }
}
