# Atoma 项目功能分析

本文档旨在分析 `atoma` 项目的核心功能、架构和工作流程。分析主要基于 `atoma-api`、`atoma-client`、`atoma-core` 和 `atoma-storage-mongo` 四个核心模块。

## 一、 项目概述

`atoma` 是一个为分布式环境设计的 Java 协调框架。它提供了常见的并发同步原语（如分布式锁、读写锁、信号量等），使得在多个独立服务或进程之间安全、可靠地协调对共享资源的访问成为可能。

该项目的核心设计思想是**基于租约（Lease）的资源管理**和**可插拔的后端存储**。通过租约机制，系统可以自动处理客户端崩溃或网络分区等故障场景，避免死锁。通过将核心逻辑与存储实现分离，项目拥有良好的扩展性，允许替换不同的存储后端（例如，从 MongoDB 切换到 ZooKeeper 或 Etcd）。

## 二、 核心模块分析

### 1. `atoma-api`

此模块是整个框架的**抽象契约层**。它定义了所有组件和用户需要遵守的公共接口和数据模型，实现了核心功能与具体实现的分离。

- **核心接口**:
    - `Lease` / `Leasable`: 定义了基于时间的租约模型。任何可被“拥有”的资源（如锁）都是 `Leasable` 的。获取资源会得到一个 `Lease` 对象，该对象代表了在特定时间段内的所有权。客户端必须在租约到期前主动续约（`renew`）以维持所有权，或在完成时释放（`release`）。
    - `Lock` / `ReadWriteLock` / `Semaphore`: 定义了标准的分布式同步原语接口。这些接口与 Java 本地的 `java.util.concurrent` 包中的对应物非常相似，但它们的方法（如 `tryAcquire(Duration leaseTime)`）都融入了租约概念。
    - `CoordinationStore`: 这是后端存储的**核心抽象**。它定义了一套底层的、原子的协调操作，如 `put`（放置/获取资源）、`get`（获取资源信息）、`renew`（续约）、`release`（释放）等。所有上层的同步原语最终都通过调用 `CoordinationStore` 的接口来实现。

### 2. `atoma-client`

此模块是面向最终用户的**客户端入口**。它封装了与后端交互的复杂性，为开发者提供了简单、易用的 API。

- **主要组件**:
    - `AtomaClient`: 这是与 `atoma` 系统交互的主要入口点。开发者首先需要用一个具体的 `CoordinationStore` 实现（如 `MongoCoordinationStore`）来实例化 `AtomaClient`。然后，通过这个客户端实例，可以像工厂一样创建出所需的分布式同步工具，例如 `client.newLock("my-lock")` 或 `client.newSemaphore("my-semaphore", 5)`。

### 3. `atoma-core`

此模块（根据结构推断）包含了独立于特定存储实现的**核心共享逻辑**。它可能是连接 `atoma-api` 的抽象定义和 `atoma-client` 的用户操作之间的桥梁，处理诸如线程管理、重试逻辑、租约续约调度等通用任务。

### 4. `atoma-storage-mongo`

此模块是 `CoordinationStore` 接口的一个**具体实现**，它使用 MongoDB 作为后端分布式协调服务。

- **主要组件**:
    - `MongoCoordinationStore`: 该类实现了 `CoordinationStore` 接口。它将抽象的协调操作（如 `put`、`renew`）转化为对 MongoDB 的具体文档操作（如 `findOneAndUpdate`、`insertOne` 等）。
    - **工作机制**: 它将每一个分布式资源（如一个名为 "my-lock" 的锁）表示为 MongoDB 集合中的一个文档。通过利用 MongoDB 的原子更新操作来确保并发场景下的数据一致性。租约的实现则是在文档中存储一个 `ttl`（过期时间）字段，并通过后台任务或客户端主动续约来更新这个时间。当客户端崩溃时，其持有的租约会自动过期，从而释放资源，避免系统死锁。

## 三、 整体工作流程（以分布式锁为例）

1.  **初始化**:
    - 应用程序创建一个 `MongoCoordinationStore` 实例，配置好 MongoDB 的连接信息。
    - 使用此 `store` 实例创建一个 `AtomaClient`。

2.  **获取锁**:
    - 应用程序通过 `client.newLock("my-distributed-lock")` 获取一个 `Lock` 对象。
    - 调用 `lock.tryAcquire(Duration.ofSeconds(30))` 尝试获取一个租期为 30 秒的锁。

3.  **后端交互**:
    - `Lock` 对象内部通过 `AtomaClient` 调用 `MongoCoordinationStore` 的 `put` 方法。
    - `MongoCoordinationStore` 尝试向 MongoDB 中一个特定的集合原子性地插入或更新一个文档。该文档的 `_id` 可能就是 "my-distributed-lock"。
    - 它会尝试设置文档中的 `ownerId` 为当前客户端的唯一标识，并设置 `expireAt` 字段为当前时间 + 30 秒。
    - 只有当该文档不存在或其租约已过期时，这个原子操作才能成功。

4.  **持有与续约**:
    - 如果 `tryAcquire` 返回 `true`，客户端成功获得锁。
    - 客户端需要在 30 秒内，通过 `lease.renew()` 方法主动续约，以延长锁的持有时间。续约操作会更新 MongoDB 中对应文档的 `expireAt` 字段。

5.  **释放与故障恢复**:
    - 任务完成后，客户端调用 `lock.release()`，这将从 MongoDB 中删除对应的文档或清空其 `ownerId`。
    - 如果客户端在持有锁期间崩溃，它将无法续约。当 `expireAt` 时间到达后，其他客户端就可以通过步骤 3 的原子操作成功获取该锁，从而实现了故障自动恢复。
