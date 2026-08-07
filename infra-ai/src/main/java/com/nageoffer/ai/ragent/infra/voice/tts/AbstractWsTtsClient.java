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

import com.nageoffer.ai.ragent.infra.model.ModelTarget;
import com.nageoffer.ai.ragent.infra.voice.VoiceConnection;
import com.nageoffer.ai.ragent.infra.voice.VoiceStreamCallback;
import com.nageoffer.ai.ragent.infra.voice.WsExecutorConfig;
import com.nageoffer.ai.ragent.infra.voice.WsTaskExecutor;
import com.nageoffer.ai.ragent.infra.voice.WsTaskSession;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 基于 WebSocket 的 TTS 供应商公共模板
 */
public abstract class AbstractWsTtsClient<P, C extends VoiceConnection<P, String, byte[]>>
        implements TtsClient, AutoCloseable {

    private final WsTaskExecutor<P, String, byte[], C> taskExecutor;

    protected AbstractWsTtsClient(Executor executor, WsExecutorConfig poolConfig) {
        this.taskExecutor = new WsTaskExecutor<>(this::createConnection, poolConfig, executor);
    }

    @Override
    public final TtsTask synthesize(TtsRequest request, TtsCallback callback, ModelTarget target) {
        AtomicBoolean audioReceived = new AtomicBoolean();
        VoiceStreamCallback<byte[]> streamCallback = adaptCallback(callback, target, audioReceived);
        WsTaskSession<String> session = taskExecutor.openTask(
                target,
                buildTaskParam(request, target),
                streamCallback,
                () -> !audioReceived.get()
        );
        try {
            session.send(request.text());
            session.finish().whenComplete((ignored, throwable) -> {
                if (throwable != null) {
                    streamCallback.onError(throwable);
                }
            });
            return wrapTask(session);
        } catch (RuntimeException exception) {
            session.cancel();
            throw exception;
        }
    }

    protected abstract P buildTaskParam(TtsRequest request, ModelTarget target);

    protected abstract C createConnection(ModelTarget target) throws Exception;

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

    private TtsTask wrapTask(WsTaskSession<String> session) {
        return new TtsTask() {
            @Override
            public void cancel() {
                session.cancel();
            }

            @Override
            public void cancelAndInvalidate() {
                session.cancelAndInvalidate();
            }
        };
    }

    @Override
    public final void close() {
        taskExecutor.close();
    }
}
