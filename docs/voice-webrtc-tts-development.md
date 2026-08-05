# 消息 TTS 流式播放开发文档

## 1. 当前链路

消息 TTS 使用供应商原生 Ogg Opus 音频，通过 HTTP chunked 流式返回。浏览器端
先用 Ogg demux 提取 Opus packet，再由 WebCodecs 解码为 Float32，最后交给
AudioWorklet 播放。这样压缩传输和播放缓冲已经分层，后续 WebRTC 可以直接复用
供应商产生的 Opus packet。

```text
TTS provider -> Ogg Opus chunks -> HTTP chunked response -> Fetch
             -> Ogg demux -> WebCodecs AudioDecoder
             -> Float32 -> AudioWorklet -> AudioContext destination
```

百炼 `run-task` 只发送协议必选的 `text_type`、`voice`，并指定 `format=opus`；
不发送采样率、声道、音量、语速、音高和码率等可选参数。前端按 Opus 单声道流
完成解码和播放。

裸 Opus packet 不能在 HTTP 中无边界拼接，也不能直接交给 AudioWorklet。当前协议
保留 Ogg 页的边界和元数据，前端自行拆页、提取 packet；这个 Ogg framing 只是
HTTP 流的容器，不等同于 WebRTC RTP framing。

## 2. 后端分层

```text
VoiceTtsController
        |
MessageSpeechService
        |
TtsService -> RoutingTtsService -> ModelRoutingExecutor.executeWsWithFallback
                                      |
                              WsModelExecutorRegistry
                                      |
                              TtsClient.startWsAttempt
                                      |
                              AbstractTtsClient
                                      |
                          DashScopeTtsConnectionFactory
```

* `VoiceTtsController`：提供 `GET /voice/tts/stream?messageId=...`，返回
  `audio/ogg; codecs=opus`。响应线程等待当前流完成或客户端断开。
* `MessageSpeechService`：校验消息归属、角色和删除状态，直接使用消息原文启动 TTS，
  不感知 HTTP 播放器或未来的 WebRTC。
* `RoutingTtsService`：复用 `ModelSelector` 的候选顺序，调用新增的
  `executeWsWithFallback`。首个非空 Opus 数据到达前，事件先缓冲；首包错误、
  超时或空完成时取消当前 Attempt 并切换下一个模型。首包提交后固定当前模型，
  不再自动 fallback。
* `WsModelExecutorRegistry`：按 `capability:modelId` 创建独立固定线程池。WS
  worker 覆盖从连接借用、首包探测、持续回调到终态清理的完整生命周期；默认使用
  `SynchronousQueue`，线程池满时立即返回容量失败并尝试备用模型。
* `AbstractTtsClient`：实现 `startWsAttempt`，在 WS worker 中直接借用连接并启动
  供应商流；当前只保留这一条流式执行链路。
* `TtsConnection` / `TtsConnectionFactory`：抽象连接池和供应商协议，连接正常完
  成后归还对象池，错误或超时后失效销毁。
* `BaiLianTtsRequestFactory`：覆盖 DashScope SDK 默认的参数展开行为，确保
  `run-task.parameters` 只有 `text_type=PlainText`、`voice` 和 `format=opus`。

`ModelHealthStore` 继续负责模型级熔断。首包成功调用 `markSuccess`；首包前错误、
超时和无有效音频调用 `markFailure`；线程池/连接池容量不足或用户取消会释放
HALF_OPEN 探测名额，不计为模型失败。

## 3. 前端解码和 AudioWorklet 流程

1. 点击消息播放按钮，使用 Fetch 请求 `/voice/tts/stream?messageId=...`。
2. 初始化 Opus 单声道解码器。
3. 创建 `AudioContext`，加载 `src/worklets/audioWorklet.ts`，建立
   `float-audio-processor` 节点。
4. `OggOpusDemuxer` 处理 Fetch 任意分片边界，跳过 Ogg 的前两个元数据 packet，
   提取完整 Opus packet。
5. `AudioDecoder` 解码 packet；输出帧转换为交错 Float32，通过 `MessagePort`
   transfer 给 AudioWorklet。
6. 首个解码音频帧到达后调用 `onPlaying` 并恢复 `AudioContext`，不等待整段语音。
7. HTTP 结束后 flush 解码器，发送 `end`，等待 Worklet 发出 `ended`，再释放节点和
   `AudioContext`。
8. 用户停止、切换消息或组件卸载时调用 `AbortController.abort()`，取消 Fetch、
   reader、解码器和 Worklet 队列。

当前浏览器需要同时支持 AudioWorklet 和 WebCodecs `AudioDecoder`/`EncodedAudioChunk`，
前端直接使用这些能力完成解码和播放。

## 4. 配置

```yaml
ai:
  providers:
    bailian:
      api-key: ${BAILIAN_API_KEY:}
      tts:
        voice: longanyang
  tts:
    default-model: cosyvoice-v3-flash
    candidates:
      - id: cosyvoice-v3-flash
        provider: bailian
        model: cosyvoice-v3-flash
        timeout-ms: 10000
```

模型线程池按 `capability:modelId` 隔离，连接池按 `provider + modelId + apiKey` 隔离。新增
供应商只需实现 `TtsClient.startWsAttempt`（或复用抽象客户端实现）以及请求/连接
适配器，不需要修改路由器。

## 5. 适用边界与验证

当前阶段只实现聊天消息的 HTTP Opus 播放，不实现 VAD、WebRTC 会话或麦克风
上行。后续 WebRTC 可以复用解码前的 Opus packet，或将同一 packet 交给 RTP/编码
适配层；HTTP Ogg 页不能直接作为 RTP 包发送。

建议验证：

1. 接口返回 `audio/ogg; codecs=opus`。
2. 首个解码音频帧到达后即可开始播放，不等待完整音频。
3. 首候选首包错误、超时或空完成时，失败候选的音频和终态不会泄漏，备用模型
   可以继续输出。
4. 首包成功后发生错误只结束当前流，不自动切换模型。
5. 连续失败达到阈值后跳过 OPEN 模型；HALF_OPEN 只能放行一个探测调用。
6. 客户端断开后，服务端取消当前 Attempt，WS worker 和连接池资源最终释放。
