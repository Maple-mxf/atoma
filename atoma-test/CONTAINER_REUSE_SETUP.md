# TestContainers 容器复用配置指南

## 概述

本项目配置了 TestContainers 的容器复用机制，确保在运行多个测试时，MongoDB 容器不会被重复创建和销毁，从而提高测试执行效率。

## 配置说明

### 1. 代码层面的配置

在 `BaseMutexTest.java` 中：
- 使用静态单例模式创建容器实例
- 设置 `.withReuse(true)` 启用复用
- 为容器设置固定的名称和标签

```java
static {
    mongoDBContainer = new MongoDBContainer("mongo:7.0")
            .withExposedPorts(27017)
            .withReuse(true)
            .withCreateContainerCmdModifier(cmd -> {
                cmd.withName("atoma-test-mongodb");
                cmd.getLabels().put("testcontainers.reuse", "true");
                cmd.getLabels().put("project", "atoma-test");
            });
    mongoDBContainer.start();
}
```

### 2. 系统配置文件

需要确保以下配置文件存在并包含正确的设置：

#### ~/.testcontainers.properties
```properties
testcontainers.reuse.enable=true
docker.client.strategy=org.testcontainers.dockerclient.UnixSocketClientProviderStrategy
```

#### Maven 配置 (pom.xml)
在 `maven-surefire-plugin` 插件中添加：
```xml
<configuration>
    <systemPropertyVariables>
        <testcontainers.reuse.enable>true</testcontainers.reuse.enable>
    </systemPropertyVariables>
</configuration>
```

### 3. 环境变量（可选）

可以设置环境变量：
```bash
export TESTCONTAINERS_REUSE_ENABLE=true
```

## 验证容器复用

运行提供的验证脚本：
```bash
./verify-container-reuse.sh
```

## 注意事项

1. **容器生命周期**：启用复用后，容器在测试结束后不会自动清理，需要手动清理：
   ```bash
   docker rm -f atoma-test-mongodb
   ```

2. **并行测试**：复用机制可能会影响并行测试的执行，确保测试之间数据隔离。

3. **调试信息**：如果复用不生效，可以启用 TestContainers 的调试日志：
   ```bash
   export TESTCONTAINERS_DEBUG=true
   ```

## 常见问题

### Q: 容器仍然被清理了？
A: 请检查：
- 配置文件是否正确创建
- 环境变量是否设置
- 容器名称是否固定
- TestContainers 版本是否支持复用（需要 1.15.0+）

### Q: 如何查看复用的容器？
A: 使用命令：
```bash
docker ps -a --filter "label=testcontainers.reuse=true"
```