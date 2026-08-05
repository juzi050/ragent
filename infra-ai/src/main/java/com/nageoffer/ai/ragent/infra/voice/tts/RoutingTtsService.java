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

import cn.hutool.core.collection.CollUtil;
import com.nageoffer.ai.ragent.framework.errorcode.BaseErrorCode;
import com.nageoffer.ai.ragent.framework.exception.RemoteException;
import com.nageoffer.ai.ragent.infra.enums.ModelCapability;
import com.nageoffer.ai.ragent.infra.model.ModelRoutingExecutor;
import com.nageoffer.ai.ragent.infra.model.ModelSelector;
import com.nageoffer.ai.ragent.infra.model.ModelTarget;
import com.nageoffer.ai.ragent.infra.model.WsAttemptSink;
import com.nageoffer.ai.ragent.infra.model.WsRoutingOptions;
import com.nageoffer.ai.ragent.infra.model.WsRoutingTask;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 根据配置选择 TTS 运营商客户端。
 */
@Service
@Primary
@Slf4j
public class RoutingTtsService implements TtsService {

    private static final String NO_PROVIDER_MESSAGE = "无可用 TTS 模型";

    private final ModelSelector selector;
    private final ModelRoutingExecutor executor;
    private final Map<String, TtsClient> clientsByProvider;

    public RoutingTtsService(ModelSelector selector,
                             ModelRoutingExecutor executor,
                             List<TtsClient> clients) {
        this.selector = selector;
        this.executor = executor;
        this.clientsByProvider = clients.stream()
                .collect(Collectors.toMap(
                        client -> client.provider().toLowerCase(Locale.ROOT),
                        Function.identity()
                ));
    }

    @Override
    public TtsTask streamTts(String text, TtsStreamListener listener) {
        List<ModelTarget> targets = selector.selectTtsCandidates();
        if (CollUtil.isEmpty(targets)) {
            throw new RemoteException(NO_PROVIDER_MESSAGE, BaseErrorCode.REMOTE_ERROR);
        }

        WsAttemptSink<byte[]> observer = new WsAttemptSink<>() {
            @Override
            public void onEvent(byte[] event) {
                listener.onAudio(event);
            }

            @Override
            public void onComplete() {
                listener.onComplete();
            }

            @Override
            public void onError(Throwable throwable) {
                listener.onError(throwable);
            }
        };

        WsRoutingTask<byte[]> task = executor.executeWsWithFallback(
                ModelCapability.TTS,
                targets,
                this::resolveClientOrNull,
                (client, target, sink) -> client.startWsAttempt(
                        text,
                        new TtsStreamListener() {
                            @Override
                            public void onAudio(byte[] audio) {
                                if (audio != null && audio.length > 0) {
                                    sink.onEvent(audio);
                                }
                            }

                            @Override
                            public void onComplete() {
                                sink.onComplete();
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                sink.onError(throwable);
                            }
                        },
                        target
                ),
                audio -> audio != null && audio.length > 0,
                observer,
                WsRoutingOptions.defaults()
        );

        // 路由和首包探测完全由 WS Runtime 异步执行；调用方只拿取消句柄，
        // 音频、完成和错误均沿流式 observer 回调返回。
        return task::cancel;
    }

    private TtsClient resolveClientOrNull(ModelTarget target) {
        String provider = target.candidate().getProvider().toLowerCase(Locale.ROOT);
        TtsClient client = clientsByProvider.get(provider);
        if (client == null) {
            log.warn("TTS 运营商客户端缺失。modelId: {}, provider: {}", target.id(), provider);
        }
        return client;
    }

}
