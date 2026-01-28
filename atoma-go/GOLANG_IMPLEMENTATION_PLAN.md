# Atoma Golang 版本实现计划 (增强版)

本文档基于对现有 Java 版本 `atoma` 项目的分析，旨在为实现其 Golang 版本提供一个清晰、分步的开发计划。计划将遵循 Go 语言的编程范式，确保最终实现是健壮、高效且易于维护的。

## 一、 核心目标

- **功能对等**: 实现与 Java 版本相同核心功能的分布式同步原语（锁、读写锁、信号量、倒计时门闩等）。
- **Go 语言范式**: 全面采用 Go 的并发模型（goroutine、channel）、错误处理机制和上下文管理（`context.Context`）。
- **模块化设计**: 保持与原项目类似的模块化和可扩展性，特别是 `CoordinationStore` 的可插拔设计。
- **高可靠性**: 完整实现基于租约（Lease）的故障恢复机制，包括租约的自动续期和过期后的自动释放。

## 二、 建议项目结构

```
atoma-go/
├── go.mod
├── api/
│   ├── lease.go         // 定义 Lease 接口和相关结构体
│   ├── synchronizer.go  // 定义 Lock, ReadWriteLock, Semaphore, CountDownLatch 等接口
│   └── store.go         // 定义 CoordinationStore 接口和 Resource 结构体
├── client/
│   └── client.go        // 定义 AtomaClient 客户端及其工厂方法
├── internal/
│   ├── countdownlatch/  // CountDownLatch 接口的具体实现
│   ├── lease/           // 租约自动续期的核心逻辑
│   ├── lock/            // Lock 接口的具体实现
│   ├── rwlock/          // ReadWriteLock 接口的具体实现
│   └── semaphore/       // Semaphore 接口的具体实现
├── storage/
│   └── mongo/
│       └── mongo_store.go // 为 MongoDB 实现 CoordinationStore 接口
└── examples/
    └── main.go          // 提供一个如何使用本库的示例
```

## 三、 分步实现计划

### 第 1 步：定义核心接口 (`api` 包)

1.  **`api/store.go`**:
    - `Resource` 结构体需要扩展，以支持不同类型的原语。可以添加 `Type` (lock, semaphore, etc.) 和 `Data` (map[string]any) 字段来存储特定于类型的数据（如信号量的总许可数）。
    - `CoordinationStore` 接口需要扩展以支持更丰富的操作。
      ```go
      // (保持 Acquire, Release, Renew, Get 不变, 通过 Resource.Data 传递额外参数)
      // 新增 CountDownLatch 相关方法
      type CoordinationStore interface {
          // ... 原有方法
          CountDown(ctx context.Context, resourceName string) (int64, error)
      }
      ```

2.  **`api/synchronizer.go`**:
    - 扩展此文件，添加 `Semaphore`, `ReadWriteLock`, `CountDownLatch` 的接口定义。
      ```go
      // Lock 接口 (不变)
      type Lock interface { ... }

      // 新增 ReadWriteLock 接口
      type ReadWriteLock interface {
          ReadLock() Lock
          WriteLock() Lock
      }

      // 新增 Semaphore 接口
      type Semaphore interface {
          Acquire(ctx context.Context, n int, leaseTime time.Duration) (*Lease, error)
          TryAcquire(ctx context.Context, n int, leaseTime time.Duration) (*Lease, error)
          Release(ctx context.Context, lease *Lease) error
      }

      // 新增 CountDownLatch 接口
      type CountDownLatch interface {
          Await(ctx context.Context) error
          CountDown(ctx context.Context) error
          GetCount(ctx context.Context) (int64, error)
      }
      ```

### 第 2 步：扩展 MongoDB 存储后端 (`storage/mongo` 包)

`MongoStore` 的实现需要变得更通用，以处理不同类型的资源。

1.  **BSON 结构体**: `bsonResource` 需要能表示所有类型的原语。
    ```go
    type bsonResource struct {
        Name         string        `bson:"_id"`
        Type         string        `bson:"type"` // "lock", "semaphore", "rwlock", "countdownlatch"
        // Lock / WriteLock
        WriteOwner   *LeaseInfo    `bson:"writeOwner,omitempty"`
        // ReadLock
        ReadOwners   []*LeaseInfo  `bson:"readOwners,omitempty"`
        // Semaphore
        TotalPermits int           `bson:"totalPermits,omitempty"`
        Owners       []*LeaseInfo  `bson:"owners,omitempty"` // 持有许可的租约
        // CountDownLatch
        Count        int64         `bson:"count,omitempty"`
    }
    type LeaseInfo struct {
        LeaseID    string    `bson:"leaseId"`
        OwnerID    string    `bson:"ownerId"`
        ExpireAt   time.Time `bson:"expireAt"`
        Permits    int       `bson:"permits,omitempty"` // for semaphore
    }
    ```

2.  **`Acquire` 方法重构**:
    - `Acquire` 方法需要根据请求的资源类型（通过 `Resource.Type` 和 `Resource.Data` 传入）来构建不同的 MongoDB 查询。
    - **Semaphore `Acquire(n)` 逻辑**:
        - **Filter**: 查询 `_id` 匹配，并且 `len(Owners) + n <= TotalPermits`。这在 MongoDB 中不易直接实现。替代方案：使用聚合管道更新或更简单的模型，即文档中保存 `availablePermits`。Filter: `availablePermits >= n`。
        - **Update**: `$inc: { availablePermits: -n }` 并 `$push: { owners: { ...lease info... } }`。
    - **ReadWriteLock `ReadLock.Acquire` 逻辑**:
        - **Filter**: `_id` 匹配，且 `writeOwner` 不存在。
        - **Update**: `$push: { readOwners: { ...lease info... } }`。
    - **ReadWriteLock `WriteLock.Acquire` 逻辑**:
        - **Filter**: `_id` 匹配，且 `writeOwner` 不存在，`readOwners` 为空或 `[]`。
        - **Update**: `$set: { writeOwner: { ...lease info... } }`。

3.  **新增 `CountDown` 方法**:
    - **Filter**: `_id` 匹配。
    - **Update**: `$inc: { count: -1 }`。确保 `count` 不会低于 0。
    - 返回更新后的 `count`。

### 第 3 步：实现租约自动续期 (`internal/lease` 包)

此部分逻辑基本可重用，无需大的改动。

### 第 4 步：实现新增的同步原语

1.  **`internal/semaphore/semaphore.go`**:
    - 实现 `api.Semaphore` 接口。
    - `Acquire` 方法调用 `store.Acquire`，并在 `Resource.Data` 中传入信号量所需的参数（如请求的许可数 `n`）。
    - 成功获取后，同样启动租约续期 goroutine。

2.  **`internal/rwlock/rwlock.go`**:
    - 实现 `api.ReadWriteLock` 接口。
    - `ReadLock()` 和 `WriteLock()` 方法返回实现了 `api.Lock` 接口的内部 `readLock` 和 `writeLock` 对象。
    - `readLock` 和 `writeLock` 对象的 `Acquire` 方法在调用 `store.Acquire` 时，会传入不同的资源类型或参数，以触发 `MongoStore` 中对应的读锁/写锁逻辑。

3.  **`internal/countdownlatch/countdownlatch.go`**:
    - 实现 `api.CountDownLatch` 接口。
    - **`CountDown`**: 调用 `store.CountDown` 方法。
    - **`Await`**: 实现轮询逻辑。
        - 启动一个循环，在其中调用 `store.Get` 来获取资源状态。
        - 如果 `count > 0`，则等待一小段时间（例如 `time.Sleep(1 * time.Second)`）后重试。
        - 循环应在 `ctx.Done()` 触发时中止。
    - **`GetCount`**: 调用 `store.Get` 并返回 `count`。

### 第 5 步：扩展客户端入口 (`client` 包)

在 `client/client.go` 中添加新的工厂方法。

```go
// ... NewLock 方法保持不变

func (c *AtomaClient) NewReadWriteLock(resourceName string) api.ReadWriteLock {
    return rwlock.New(c.store, resourceName)
}

func (c *AtomaClient) NewSemaphore(resourceName string, initialPermits int) api.Semaphore {
    return semaphore.New(c.store, resourceName, initialPermits)
}

func (c *AtomaClient) NewCountDownLatch(resourceName string, count int64) api.CountDownLatch {
    return countdownlatch.New(c.store, resourceName, count)
}
```

### 第 6 步：编写测试和示例

- **扩展测试**: 为 `Semaphore`, `ReadWriteLock`, `CountDownLatch` 编写专门的单元测试和集成测试，验证其并发正确性和故障恢复能力。
- **扩展示例**: 在 `examples/main.go` 中添加使用新原语的示例代码。

## 四、 总结

遵循以上增强计划，我们可以构建一个功能更全面、代码风格地道、且充分利用了 Go 语言并发优势的 `atoma` 库。整个设计将保持模块化、可测试和可扩展性。