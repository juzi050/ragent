/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nageoffer.ai.ragent.infra.voice.tts;

import com.nageoffer.ai.ragent.framework.errorcode.BaseErrorCode;
import com.nageoffer.ai.ragent.framework.exception.RemoteException;
import com.nageoffer.ai.ragent.infra.chat.StreamCancellationHandle;
import com.nageoffer.ai.ragent.infra.http.ModelClientErrorType;
import com.nageoffer.ai.ragent.infra.http.ModelClientException;
import com.nageoffer.ai.ragent.infra.model.ModelHealthStore;
import com.nageoffer.ai.ragent.infra.model.ModelSelector;
import com.nageoffer.ai.ragent.infra.model.ModelTarget;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 在 TTS 候选模型间执行健康检查、首音频确认和 fallback
 */
@Slf4j
@Service
@Primary
public class RoutingTtsService implements TtsService {

    private final ModelSelector selector;
    private final ModelHealthStore healthStore;
    private final Map<String, TtsClient> clientsByProvider;

    public RoutingTtsService(ModelSelector selector,
                             ModelHealthStore healthStore,
                             List<TtsClient> clients) {
        this.selector = selector;
        this.healthStore = healthStore;
        this.clientsByProvider = clients.stream()
                .collect(Collectors.toMap(TtsClient::provider, Function.identity()));
    }

    @Override
    public StreamCancellationHandle synthesize(String text, TtsCallback callback, TtsTaskObserver taskObserver) {
        List<ModelTarget> targets = selector.selectTtsCandidates();
        if (targets.isEmpty()) {
            throw notifyAllFailed(callback, null);
        }

        Throwable lastError = null;
        for (ModelTarget target : targets) {
            TtsClient client = resolveClient(target);
            if (client == null) {
                continue;
            }
            ModelHealthStore.CallPermit permit = healthStore.allowCall(target.id());
            if (permit == null) {
                continue;
            }

            try {
                BinaryProbeStreamBridge bridge = new BinaryProbeStreamBridge(callback);
                StreamCancellationHandle handle;
                try {
                    handle = client.synthesize(text, bridge, target);
                } catch (RuntimeException exception) {
                    bridge.discard();
                    lastError = exception;
                    if (exception instanceof ModelClientException clientException
                            && clientException.getErrorType() == ModelClientErrorType.CAPACITY_EXHAUSTED) {
                        log.warn("TTS 连接池容量不足，modelId={}", target.id());
                        continue;
                    }
                    healthStore.markFailure(target.id());
                    log.warn("TTS 任务启动失败，modelId={}，provider={}",
                            target.id(), target.candidate().getProvider(), exception);
                    continue;
                }
                if (handle == null) {
                    bridge.discard();
                    healthStore.markFailure(target.id());
                    lastError = new ModelClientException("TTS 供应商未返回任务句柄，modelId=" + target.id(),
                            ModelClientErrorType.INVALID_RESPONSE, null);
                    continue;
                }

                taskObserver.onTaskStarted(handle);
                if (taskObserver.isCancelled()) {
                    bridge.discard();
                    return handle;
                }

                BinaryProbeStreamBridge.ProbeResult result = awaitFirstAudio(bridge, handle, target);
                if (taskObserver.isCancelled()) {
                    bridge.discard();
                    return handle;
                }
                if (result.isSuccess()) {
                    healthStore.markSuccess(target.id());
                    bridge.commit();
                    return handle;
                }

                bridge.discard();
                healthStore.markFailure(target.id());
                lastError = buildFailure(target, result);
                cancelQuietly(handle, target);
                log.warn("TTS 首音频确认失败，modelId={}，type={}",
                        target.id(), result.getType(), lastError);
            } finally {
                // 用户取消、连接池耗尽等中性退出不记模型故障，但必须归还半开探测名额
                healthStore.releaseHalfOpenPermit(permit);
            }
        }

        throw notifyAllFailed(callback, lastError);
    }

    private TtsClient resolveClient(ModelTarget target) {
        TtsClient client = clientsByProvider.get(target.candidate().getProvider());
        if (client == null) {
            log.warn("TTS 提供商客户端缺失: provider={}，modelId={}",
                    target.candidate().getProvider(), target.id());
        }
        return client;
    }

    private BinaryProbeStreamBridge.ProbeResult awaitFirstAudio(BinaryProbeStreamBridge bridge,
                                                                StreamCancellationHandle handle,
                                                                ModelTarget target) {
        try {
            long timeoutMs = target.timeoutMs();
            return bridge.awaitFirstAudio(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            RemoteException interrupted = new RemoteException(
                    "TTS 首音频等待被中断", exception, BaseErrorCode.REMOTE_ERROR);
            bridge.onError(interrupted);
            bridge.commit();
            cancelQuietly(handle, target);
            throw interrupted;
        }
    }

    private Throwable buildFailure(ModelTarget target, BinaryProbeStreamBridge.ProbeResult result) {
        return switch (result.getType()) {
            case ERROR -> result.getError() != null
                    ? result.getError()
                    : new ModelClientException("TTS 流式任务失败，modelId=" + target.id(),
                    ModelClientErrorType.PROVIDER_ERROR, null);
            case TIMEOUT -> new ModelClientException("TTS 首音频超时，modelId=" + target.id(),
                    ModelClientErrorType.NETWORK_ERROR, null);
            case NO_CONTENT -> new ModelClientException("TTS 未返回有效音频，modelId=" + target.id(),
                    ModelClientErrorType.INVALID_RESPONSE, null);
            case SUCCESS -> throw new IllegalArgumentException("成功结果不应构建失败异常");
        };
    }

    private RuntimeException notifyAllFailed(TtsCallback callback, Throwable lastError) {
        RemoteException failure = new RemoteException(
                "所有 TTS 模型均调用失败",
                lastError,
                BaseErrorCode.REMOTE_ERROR
        );
        callback.onError(failure);
        return failure;
    }

    private void cancelQuietly(StreamCancellationHandle handle, ModelTarget target) {
        try {
            handle.cancel();
        } catch (RuntimeException exception) {
            log.warn("TTS 失败候选取消异常，modelId={}，provider={}",
                    target.id(), target.candidate().getProvider(), exception);
        }
    }

}
