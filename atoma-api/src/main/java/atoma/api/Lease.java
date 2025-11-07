package atoma.api;

/**
 * 代表一个客户端与 Atoma 协调服务之间的会话租约。
 *
 * <p>Lease 是所有分布式原语安全运行的基础。它通过自动续约机制来维持其有效性， 并在客户端故障时，保证其持有的资源（如锁）最终能够被系统自动回收，从而防止死锁。
 *
 * <p>此接口实现了 {@link AutoCloseable}，强烈建议在 try-with-resources 语句块中使用， 以确保在客户端正常退出时，租约能被干净地撤销。
 */
public interface Lease extends AutoCloseable {

  /**
   * 获取此租约的全局唯一标识符。 这个 ID 将被用于标识资源的所有权。
   *
   * @return 租约的唯一 ID。
   */
  String getId();

  /**
   * 明确地、永久性地撤销此租约。
   *
   * <p>调用此方法会立即通知协调服务此租约已终止，并触发所有与此租约相关的资源的清理工作。 这是一个幂等操作。
   */
  void revoke();

  /**
   * 检查此租约是否已被客户端明确撤销
   *
   * <p>注意：返回 {@code false} 并不保证租约在服务端仍然有效，因为它可能因为网络分区等原因而过期。 此方法只反映客户端是否主动调用了 {@link #revoke()}。
   *
   * @return 如果 {@link #revoke()} 已被调用，则返回 true。
   */
  boolean isRevoked();

  /** 实现 AutoCloseable 接口，其行为等同于调用 {@link #revoke()}。 */
  @Override
  default void close() {
    revoke();
  }
}
