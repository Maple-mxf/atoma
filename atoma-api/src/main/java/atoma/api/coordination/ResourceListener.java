package atoma.api.coordination;

/**
 * 一个回调接口，用于接收 Resource 的状态变更事件。 这是一个函数式接口，可以使用 lambda 表达式来实现。
 *
 * @see atoma.api.coordination.CoordinationStore#subscribe(String, String, ResourceListener)
 */
public interface ResourceListener {

  /**
   * @param event 触发事件后生成的数据对象
   */
  void onEvent(ResourceChangeEvent event);
}
