package atoma.api;

/** 基础接口：只关心资源本身 */
public interface Resourceful {

  /**
   * @return 返回资源的唯一ID
   */
  String getResourceId();
}
