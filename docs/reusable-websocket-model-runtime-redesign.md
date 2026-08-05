# 可复用 WebSocket 模型运行池：`executeWsWithFallback` 设计

## 1. 设计结论

本次改造不修改现有 `executeWithFallback`，也不为了 WebSocket 调整现有 ChatModel 调用链。

新增：

```java
executeWsWithFallback(...)
```

该方法专门处理 TTS WebSocket 长连接流式任务。与现有架构真正不兼容的只有执行线程模型：普通模型调用在线程中完成一次请求即可返回；WS 任务需要让模型线程覆盖首包探测、持续流式输出、任务结束和连接回收的完整生命周期。

其余能力继续复用：

* `ModelSelector` 和现有候选优先级；
* `ModelTarget`；
* `ModelHealthStore` 及现有熔断状态；
* `clientResolver` 和供应商客户端注册方式；
* 首包探测逻辑；
* TTS 协议客户端；
* WebSocket 连接池和连接借还规则；
* 现有异常分类、日志字段和监控体系。

总体结构：

```text
RoutingTtsService
    │
    ├── ModelSelector                    复用
    ├── ModelTarget                      复用
    │
    ▼
ModelRoutingExecutor
    │
    ├── executeWithFallback              原方法完全不变
    │
    └── executeWsWithFallback            新增 WS 流式入口
            │
            ├── ModelHealthStore         复用
            ├── WsModelExecutorRegistry  新增：每模型线程池
    ├── ModelRoutingExecutor 内部 WsSession  新增：首包前降级
            └── TtsClient                复用协议与连接池
```

## 2. 兼容边界

### 2.1 不修改的部分

以下接口和行为保持原样：

```text
ModelRoutingExecutor.executeWithFallback
ModelSelector
ModelTarget
ModelHealthStore.allowCall
ModelHealthStore.markSuccess
ModelHealthStore.markFailure
```

原有 Chat、Embedding、Rerank、VLM 等调用不进入 WS 线程池，也不感知 `WsSession`。

### 2.2 新增的部分

只增加 WS 长连接所需的对象：

```text
executeWsWithFallback
WsModelExecutorRegistry
ModelRoutingExecutor.WsSession
WsAttempt
WsAttemptHandle
WsAttemptSink
WsRoutingTask
```

如果现有方法的语义与 WS 线程池所有权冲突，不修改原方法，而是新增 WS 专用方法。
TTS 当前只实现流式链路，由 `startWsAttempt` 直接在 `executeWsWithFallback` 的
worker 中执行，不再维护第二套普通模型线程池。

## 3. `executeWsWithFallback` 接口

建议接口：

```java
public <C, E> WsRoutingTask<E> executeWsWithFallback(
        ModelCapability capability,
        List<ModelTarget> targets,
        Function<ModelTarget, C> clientResolver,
        WsModelCaller<C, E> caller,
        WsFirstPacketPolicy<E> firstPacketPolicy,
        WsAttemptSink<E> observer,
        WsRoutingOptions options);
```

相关抽象：

```java
@FunctionalInterface
public interface WsModelCaller<C, E> {
    WsAttemptHandle start(C client,
                          ModelTarget target,
                          WsAttemptSink<E> sink) throws Exception;
}

public interface WsAttemptHandle {
    CompletionStage<Void> terminal();
    void cancel();
}

public interface WsAttemptSink<E> {
    void onEvent(E event);
    void onComplete();
    void onError(Throwable cause);
}

public interface WsRoutingTask<E> {
    CompletionStage<Void> ready();
    CompletionStage<Void> completion();
    void cancel();
}
```

`WsModelCaller.start` 必须直接在当前 WS 模型线程中启动任务，不得再提交另一层模型线程池。

## 4. 线程池设计

### 4.1 唯一新增的不兼容执行机制

`WsModelExecutorRegistry` 按以下键维护线程池：

```text
RuntimeKey = capability + ":" + modelId
```

例如：

```text
tts:cosyvoice-v3-flash
tts:qwen-audio-tts
```

每个 RuntimeKey 对应一个固定线程池：

```text
threads = 该模型最大活动 WS Attempt 数
queueCapacity = 有界等待容量
rejectionPolicy = AbortPolicy
```

对于实时语音任务，默认建议：

```text
queueCapacity = 0
```

即使用 `SynchronousQueue` 快速拒绝，把任务转移到下一个候选模型。若业务允许短暂排队，可以配置小队列，但必须设置排队等待上限。

### 4.2 worker 覆盖完整生命周期

线程池中的一个 worker 对应一个活动 Attempt，并一直执行到终态：

```text
启动 WS 任务
    │
    ▼
等待首个有效业务包
    │
    ▼
持续流式处理
    │
    ▼
发送 finish-task / cancel
    │
    ▼
等待 task-finished
    │
    ▼
归还或淘汰连接
    │
    ▼
worker 释放
```

不能在底层 SDK 刚返回任务句柄时就释放 worker，否则线程数只限制“启动速度”，不能限制实际 WS 并发。

### 4.3 避免双重线程池

不采用以下双重线程池结构：

```text
executeWsWithFallback
    -> WsModelExecutorRegistry 对应线程池
        -> AbstractVoiceModelClient 内部线程池
            -> WebSocket
```

正确结构：

```text
executeWsWithFallback
    -> WsModelExecutorRegistry 对应线程池
        -> startWsAttempt
            -> WebSocket
```

`startWsAttempt` 复用请求构建、连接池、协议回调和取消逻辑，只由 WS Runtime
线程池持有完整任务生命周期。

## 5. 路由执行流程

`executeWsWithFallback` 复用现有候选顺序和健康状态，增加线程池接纳与首包状态机：

```text
获取 ModelSelector 返回的有序 targets
    │
    ▼
解析当前 target 的客户端
    │
    ├── client 缺失 -> 下一候选
    │
    ▼
提交 target 对应的 WsModelExecutorRegistry 线程池
    │
    ├── 线程池拒绝 -> CAPACITY_FULL -> 下一候选
    │
    ▼
worker 调用 ModelHealthStore.allowCall
    │
    ├── false -> CIRCUIT_OPEN -> 下一候选
    │
    ▼
调用 WsModelCaller.start
    │
    ▼
等待首包
    │
    ├── 首包前失败/超时
    │      ├── markFailure
    │      ├── 取消当前 Attempt
    │      ├── 释放或淘汰连接
    │      └── 下一候选
    │
    └── 首包成功
           ├── markSuccess
           ├── 提交首包缓冲
           ├── 绑定当前 Attempt
           └── 后续不再自动切换模型
```

线程池接纳发生在 `allowCall` 之前，因此线程池拒绝时不会占用 HALF_OPEN 探测名额，也不需要修改现有断路器接口。

## 6. 路由会话状态

`ModelRoutingExecutor` 内部的 `WsSession` 表示一次完整业务请求：

```text
TRYING
    │
    ├── 所有候选失败 -> FAILED
    │
    ├── 用户取消 -> CANCELLED
    │
    └── 首包成功 -> COMMITTED
                      │
                      ├── 正常完成 -> SUCCEEDED
                      ├── 用户取消 -> CANCELLED
                      └── 首包后异常 -> FAILED
```

核心不变量：

* 一个逻辑任务同一时刻只绑定一个 Attempt；
* 前一个 Attempt 已终态，或连接已被强制淘汰后，才能启动下一个 Attempt；
* 每个回调携带 `attemptId`，迟到事件不允许进入下游；
* `committed` 通过原子状态设置；
* 首包成功后不再进入候选遍历。

## 7. 首包缓冲和总超时

首包前的业务输出进入 Attempt 缓冲：

```text
事件到达
    │
    ├── 不是有效首包 -> 暂存或忽略
    │
    └── 是有效首包
           ├── markSuccess
           ├── committed = true
           └── 刷新缓冲到下游
```

TTS 首包由首个非空音频帧判定。

每个逻辑任务使用总 deadline：

```text
attemptTimeout = min(target.timeoutMs, routingSession.remainingTime)
```

避免多个候选分别等待完整超时时间，导致整体响应时间不可控。

## 8. 复用 `ModelHealthStore`

WS 链路继续使用现有方法：

```text
allowCall(modelId)
markSuccess(modelId)
markFailure(modelId)
```

调用规则：

| 结果 | 健康状态处理 |
| --- | --- |
| 线程池拒绝 | 不调用 `allowCall`，不计失败 |
| `allowCall=false` | 跳过当前模型 |
| 首包成功 | `markSuccess` |
| 首包超时 | `markFailure` |
| 首包前服务端错误 | `markFailure` |
| WS 首包前异常关闭 | `markFailure` |
| 无空闲连接 | 不计失败 |
| 用户取消 | 不计失败 |
| 首包后异常 | 不参与首包降级，按能力策略处理 |

如果 `allowCall` 已经放行 HALF_OPEN 探测，但任务在真正调用模型前因“无连接、取消、Runtime 关闭”等非模型原因退出，现有三个方法无法中性释放探测名额。该情况不修改已有方法语义，只新增：

```java
public void releaseCall(String modelId);
```

`releaseCall` 仅负责释放 HALF_OPEN 的 `halfOpenInFlight`，在 `CLOSED` 状态下为空操作。它是线程池/任务生命周期引入的补充方法，不创建第二套健康状态。

## 9. 连接池继续复用

连接池不需要因为新增线程池路由而重新设计。继续复用现有：

* `TtsConnection`；
* `TtsConnectionFactory`；
* `GenericObjectPool`；
* 连接借用、归还和失效规则；
* 供应商 + 凭据的连接池隔离方式。

连接处理规则保持：

| 执行结果 | 连接处理 |
| --- | --- |
| 正常收到 `task-finished` | RELEASE |
| 取消后收到 `task-finished` | RELEASE |
| 首包超时 | INVALIDATE |
| WS 异常关闭 | INVALIDATE |
| 服务端任务错误 | INVALIDATE |
| `task-finished` 超时 | INVALIDATE |

唯一需要保证的是：`startWsAttempt` 在当前 worker 内完成连接借用，并且 `terminal()` 只在连接已经完成归还或淘汰后结束。

## 10. TTS 改造后的职责

### `RoutingTtsService`

```text
selector.selectTtsCandidates()       复用
executor.executeWsWithFallback(...)  新增调用
Ogg Opus/WebCodecs/AudioWorklet 输出 由传输层消费
```

`RoutingTtsService` 不再自行遍历候选、直接操作 `ModelHealthStore` 或维护模型线程池。

### 10.1 Opus 输出边界

当前 TTS 使用供应商原生 Ogg Opus，不在服务端转码为 PCM 或 MP3：

```text
供应商 Ogg Opus -> WsAttemptSink<byte[]> -> HTTP chunked audio/ogg
                                            -> Ogg demux
                                            -> WebCodecs AudioDecoder
                                            -> Float32 AudioWorklet
```

Ogg 容器保留 Opus packet 边界；压缩字节不能直接交给 AudioWorklet，也不能去掉
边界后简单拼接。百炼 `run-task` 只发送必选的 `text_type`、`voice`，并指定
`format=opus`，不发送采样率、声道、音量、语速、音高和码率。浏览器用 WebCodecs
解码为 Float32 后交给 AudioWorklet；未来 WebRTC 可以复用解码前的 Opus packet，
但需要替换为 RTP framing，不需要修改 `executeWsWithFallback`。

### `AbstractTtsClient`

继续复用：

* 请求构建；
* API Key 校验；
* 连接池；
* 连接借还；
* 供应商回调；
* 取消逻辑。

流式链路使用直接执行入口：

```java
WsAttemptHandle startWsAttempt(...);
```

当前 TTS 不提供额外的同步或普通线程池执行入口。

## 11. 失败结果

建议 `executeWsWithFallback` 内部统一使用：

```text
ACCEPTED
CIRCUIT_OPEN
CAPACITY_FULL
NO_CONNECTION
REQUEST_REJECTED
PRE_COMMIT_FAILURE
POST_COMMIT_FAILURE
CANCELLED
```

路由动作：

| 结果 | 路由动作 |
| --- | --- |
| `CIRCUIT_OPEN` | 下一候选 |
| `CAPACITY_FULL` | 下一候选 |
| `NO_CONNECTION` | 下一候选 |
| `PRE_COMMIT_FAILURE` | 清理当前 Attempt 后下一候选 |
| `REQUEST_REJECTED` | 结束逻辑任务 |
| `POST_COMMIT_FAILURE` | 不自动切换 |
| `CANCELLED` | 结束逻辑任务 |

## 12. 测试重点

* 原有 `executeWithFallback` 测试全部不变并继续通过；
* WS 主模型线程池满时切换备用模型，且不增加失败计数；
* OPEN 模型被跳过；
* HALF_OPEN 只放行一个真实 WS Attempt；
* HALF_OPEN 任务在线程池/连接容量阶段退出时正确 `releaseCall`；
* 首包失败后当前 Attempt 完成清理，再启动备用模型；
* 首包成功后固定模型，后续异常不会启动备用模型；
* worker 在线程池中覆盖完整 WS 生命周期；
* `task-finished` 后连接归还，异常或超时后连接淘汰；
* 用户取消不会进入熔断失败计数；
* 迟到回调不会写入下游。

## 13. 实施顺序

1. 保持 `executeWithFallback` 不动，在 `ModelRoutingExecutor` 新增 `executeWsWithFallback`。
2. 新增 `WsModelExecutorRegistry`，按 `capability:modelId` 创建固定线程池。
3. 在 `ModelRoutingExecutor` 内新增 `WsSession`、`WsAttemptHandle` 和首包前状态机。
4. 复用 `ModelSelector`、`ModelTarget` 和 `ModelHealthStore`。
5. 仅为中性释放 HALF_OPEN 名额新增 `ModelHealthStore.releaseCall`，不修改已有方法。
6. 在 TTS 客户端增加不二次提交线程池的 `startWsAttempt`，复用原连接池和协议逻辑。
7. 将 `RoutingTtsService` 的候选循环替换为 `executeWsWithFallback`。
8. 验证 TTS 的首包降级、取消和连接释放链路。

最终只有线程池执行路径是新增架构；候选选择、模型配置、健康状态、客户端解析、连接池、首包判定和协议实现均继续复用现有能力。
