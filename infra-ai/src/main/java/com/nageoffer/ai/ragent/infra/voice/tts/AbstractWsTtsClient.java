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

import com.nageoffer.ai.ragent.infra.chat.StreamCancellationHandle;
import com.nageoffer.ai.ragent.infra.config.AIModelProperties;
import com.nageoffer.ai.ragent.infra.model.ModelTarget;
import com.nageoffer.ai.ragent.infra.voice.websocket.VoiceConnection;
import com.nageoffer.ai.ragent.infra.voice.websocket.VoiceStreamCallback;
import com.nageoffer.ai.ragent.infra.voice.websocket.WsTaskExecutor;
import com.nageoffer.ai.ragent.infra.voice.websocket.WsTaskSession;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 基于 WebSocket 的 TTS 供应商公共模板
 */
public abstract class AbstractWsTtsClient<P, C extends VoiceConnection<P, String, byte[]>>
        implements TtsClient, AutoCloseable {

    /**
     * continue-task 单次发送的文本长度上限
     */
    private static final int CHUNK_MAX_LEN = 80;

    private final WsTaskExecutor<P, String, byte[], C> taskExecutor;

    protected AbstractWsTtsClient(Executor executor, AIModelProperties.WebSocketConfig poolConfig) {
        this.taskExecutor = new WsTaskExecutor<>(this::createConnection, poolConfig, executor);
    }

    @Override
    public final StreamCancellationHandle synthesize(String text, TtsCallback callback, ModelTarget target) {
        AtomicBoolean audioReceived = new AtomicBoolean();
        VoiceStreamCallback<byte[]> streamCallback = adaptCallback(callback, target, audioReceived);
        WsTaskSession<String> session = taskExecutor.openTask(
                target,
                buildTaskParam(target),
                streamCallback,
                () -> !audioReceived.get()
        );
        try {
            // 分块流式发送
            for (String chunk : splitChunks(text)) {
                session.send(chunk);
            }
            session.finish().whenComplete((ignored, throwable) -> {
                if (throwable != null) {
                    streamCallback.onError(throwable);
                }
            });
            return session::cancelAndInvalidate;
        } catch (RuntimeException exception) {
            session.cancel();
            throw exception;
        }
    }

    /**
     * 按句子边界分块 达到长度上限即切分
     */
    private java.util.List<String> splitChunks(String text) {
        if (text == null || text.isEmpty()) {
            return java.util.List.of("");
        }
        java.util.List<String> chunks = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            current.append(c);
            if (isSentenceBoundary(c) || current.length() >= CHUNK_MAX_LEN) {
                chunks.add(current.toString());
                current.setLength(0);
            }
        }
        if (current.length() > 0) {
            chunks.add(current.toString());
        }
        return chunks.stream().filter(chunk -> !chunk.isBlank()).toList();
    }

    private boolean isSentenceBoundary(char c) {
        return c == '。' || c == '！' || c == '？' || c == '；' || c == '…' || c == '\n';
    }

    protected abstract P buildTaskParam(ModelTarget target);

    protected abstract C createConnection(ModelTarget target);

    protected byte[] convertAudio(byte[] providerAudio, ModelTarget target) {
        return providerAudio;
    }

    private VoiceStreamCallback<byte[]> adaptCallback(TtsCallback callback,
                                                       ModelTarget target,
                                                       AtomicBoolean audioReceived) {
        return new VoiceStreamCallback<>() {
            @Override
            protected void onValidPacket(byte[] packet) {
                if (packet.length > 0) {
                    audioReceived.set(true);
                }
                callback.onAudio(convertAudio(packet, target));
            }

            @Override
            protected void onTaskComplete() {
                callback.onComplete();
            }

            @Override
            protected void onTaskError(Throwable throwable) {
                callback.onError(throwable);
            }
        };
    }

    @Override
    public final void close() {
        taskExecutor.close();
    }
}
