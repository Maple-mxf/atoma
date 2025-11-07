package atoma.api.coordination.command;

import atoma.api.coordination.Resource;

import java.util.Optional;

/** 为 {@link CommandHandler} 提供执行所需资源的上下文。 */
public interface CommandHandlerContext {

  /**
   * @return 正在被处理的资源键。
   */
  String getResourceId();

  /**
   * @return 在执行命令前的资源节点状态。
   */
  Optional<Resource> getCurrentResource();
}
