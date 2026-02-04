# Atoma: 分布式协调原子原语库

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](https://github.com/atoma-project/atoma-java)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Maven Central](https://img.shields.io/maven-central/v/io.atoma/atoma-api.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:io.atoma)

**Atoma** 是一个使用 Java 实现的、轻量级且高性能的分布式协调原语库。它旨在将传统并发编程中广为人知的同步工具（如 `Lock`, `Semaphore`, `CountDownLatch`）引入到分布式环境中，从而帮助开发者简单、可靠地构建分布式系统。

## 简介

在复杂的分布式系统中，跨多台机器的协调与同步是一个普遍存在的难题。Atoma 提供了一套与 `java.util.concurrent` 包类似但专为分布式环境设计的 API，使得开发者可以用熟悉的方式解决分布式场景下的资源竞争、任务同步和流程控制等问题。

项目的设计哲学是**API 优先**和**可插拔后端**。核心 API (`atoma-api`) 与具体的存储实现解耦，目前官方提供了一个基于 **MongoDB** 的实现 (`atoma-storage-mongo`)。

## 核心特性

- **丰富的原语支持**:
  - **分布式锁 (`Lock`)**: 提供互斥访问，确保在任何时刻只有一个客户端可以访问共享资源。
  - **分布式读写锁 (`ReadWriteLock`)**:允许多个读操作同时进行，但写操作是互斥的，适用于读多写少的场景。
  - **分布式信号量 (`Semaphore`)**: 控制对共享资源的并发访问数量。
  - **分布式倒计时门闩 (`CountDownLatch`)**: 允许一个或多个线程等待其他线程完成操作。
  - **分布式循环栅栏 (`CyclicBarrier`)**: 让一组线程互相等待，直到所有线程都到达一个共同的屏障点。
- **可插拔的存储后端**:
  - 核心逻辑与存储层分离。
  - 内置基于 MongoDB 的 `CoordinationStore` 实现。
  - 开发者可以根据需要实现自己的存储后端，以适配不同的基础架构（如 ZooKeeper, Etcd, Redis 等）。
- **高性能与低延迟**:
  - 客户端与协调服务之间的通信经过优化，减少网络往返。
  - 利用后端存储的原子操作，确保分布式环境下操作的一致性和正确性。
- **简单易用的 API**:
  - API 设计模仿 `java.util.concurrent`，降低学习成本。
  - 提供 `AtomaClient` 作为统一的入口点，方便管理所有原语。

## 项目架构

本项目采用模块化设计，主要模块如下：

- `atoma-api`: 定义了所有分布式原语的核心接口和通用异常。这是用户和实现者都应依赖的模块。
- `atoma-core`: 提供了 Atoma 客户端的核心实现，负责与后端协调存储进行通信。
- `atoma-storage-mongo`: 基于 MongoDB 的存储层实现，实现了 `atoma-api` 中定义的 `CoordinationStore` 接口。
- `atoma-benchmark`: 包含一系列 JMH 基准测试，用于评估不同原语的性能。
- `atoma-test`: 包含项目的集成测试和单元测试套件。

## 快速开始

### 1. 先决条件

- Java 11 或更高版本。
- 一个正在运行的 MongoDB 实例。

### 2. 添加依赖

**Gradle (Kotlin DSL)**

```kotlin
// build.gradle.kts
dependencies {
    implementation("io.atoma:atoma-core:1.0.0-alpha.1")
    implementation("io.atoma:atoma-storage-mongo:1.0.0-alpha.1")
}
```

**Maven**

```xml
<!-- pom.xml -->
<dependencies>
    <dependency>
        <groupId>tech.atoma-project</groupId>
        <artifactId>atoma-core</artifactId>
        <version>1.0.0-alpha.1</version>
    </dependency>
    <dependency>
        <groupId>tech.atoma-project</groupId>
        <artifactId>atoma-storage-mongo</artifactId>
        <version>1.0.0-alpha.1</version>
    </dependency>
</dependencies>
```

### 3. 使用示例

以下是一个使用分布式互斥锁 (`MutexLock`) 的简单示例：

```java
import atoma.api.lock.Lock;
import atoma.core.AtomaClient;
import atoma.storage.mongo.MongoCoordinationStore;

public class DistributedLockExample {

    public static void main(String[] args) {
        // 1. 创建并配置一个 MongoDB 存储后端
        String connectionString = "mongodb://localhost:27017";
        String databaseName = "atoma_db";
        MongoCoordinationStore store = new MongoCoordinationStore(connectionString, databaseName);

        // 2. 创建 Atoma 客户端
        AtomaClient client = new AtomaClient.Builder()
                .address("localhost") // 标识客户端实例
                .store(store)
                .build();

        // 3. 获取一个分布式锁实例
        // "my-critical-task" 是锁的唯一名称
        Lock mutexLock = client.getMutexLock("my-critical-task");

        // 4. 在 try-finally 块中获取和释放锁，确保锁一定会被释放
        try {
            System.out.println("尝试获取锁...");
            mutexLock.lock(); // 阻塞直到获取锁
            System.out.println("成功获取锁，执行关键任务...");

            // 模拟执行任务
            Thread.sleep(10000);

            System.out.println("任务执行完毕。");

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("线程被中断");
        } finally {
            mutexLock.unlock();
            System.out.println("锁已释放。");
        }

        // 5. 关闭客户端，释放资源
        client.close();
    }
}
```

## 性能基准

为了评估 `Atoma` 原语的性能，项目包含了一系列基于 [JMH (Java Microbenchmark Harness)](https://openjdk.java.net/projects/code-tools/jmh/) 的基准测试。以下是分布式互斥锁 (`MutexLock`) 在特定环境下的性能数据摘要。

**测试环境:**

*   **后端存储**: MongoDB
*   **JVM**: OpenJDK 17.0.10

**分布式互斥锁 (`MutexLock`) 性能:**

| 基准测试 (Benchmark) | 场景 (Scenario) | 并发线程 (Threads) | 吞吐量 (Throughput) | 平均耗时 (Avg. Time) |
| :--- | :--- | :--- | :--- | :--- |
| `lockAndUnlock` | 无竞争 (No Contention) | 1 | ≈ 107 ops/sec | ≈ 8.7 ms/op |
| `lockAndUnlock` | 高竞争 (High Contention) | 32 | ≈ 177 ops/sec | ≈ 464 ms/op |

**结果分析:**

*   **无竞争**: 在单个线程、无资源竞争的理想情况下，一次完整的加锁和解锁操作平均耗时约 8.7 毫秒。这反映了与 MongoDB 后端进行一次协调操作的基本开销。
*   **高竞争**: 在 32 个线程并发争抢同一个锁的场景下，系统总吞吐量约为 177次/秒。虽然总吞吐量有所提升，但单次操作的平均耗时显著增加到 464 毫秒，这符合预期，因为大部分线程都处于等待锁释放的状态。

这些数据表明 `Atoma` 的分布式锁在提供正确互斥保障的同时，其性能开销在分布式协调场景下是合理的。用户在设计系统时应考虑到高竞争下延迟会相应增加。

## 从源码构建

1. 克隆本仓库:
   ```sh
   git clone https://github.com/your-username/atoma.git
   cd atoma
   ```

2. 使用 Gradle Wrapper 构建项目:
   ```sh
   ./gradlew build
   ```
   构建成功后，你可以在各个模块的 `build/libs` 目录下找到生成的 JAR 文件。

## 如何贡献

我们非常欢迎社区的贡献！无论是报告 Bug、提出功能建议还是提交代码。

1.  **Fork** 本仓库。
2.  创建一个新的功能分支 (`git checkout -b feature/your-feature-name`)。
3.  进行修改并提交 (`git commit -m 'Add some feature'`)。
4.  将你的分支推送到你的 Fork (`git push origin feature/your-feature-name`)。
5.  创建一个 **Pull Request**。

在提交 Pull Request 之前，请确保你的代码通过了所有测试 (`./gradlew test`)。

## 许可证

本项目采用 [Apache License 2.0](LICENSE) 许可证。
