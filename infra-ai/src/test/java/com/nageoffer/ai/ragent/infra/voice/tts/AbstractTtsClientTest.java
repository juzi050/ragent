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

import com.nageoffer.ai.ragent.infra.config.AIModelProperties;
import com.nageoffer.ai.ragent.infra.model.ModelTarget;
import com.nageoffer.ai.ragent.infra.model.WsAttemptHandle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * TTS WebSocket 客户端模板测试。
 */
class AbstractTtsClientTest {

    private TestTtsClient client;

    @BeforeEach
    void setUp() {
        client = new TestTtsClient();
    }

    @AfterEach
    void tearDown() {
        client.destroy();
    }

    @Test
    void startsWsAttemptDirectlyAndCancelsOwnedConnection() throws Exception {
        RecordingListener listener = new RecordingListener();

        WsAttemptHandle handle = client.startWsAttempt("hello", listener, target());

        assertEquals(Thread.currentThread().getName(), client.state.streamThread.get());
        assertEquals("test-api-key", client.state.request.get().apiKey());
        assertEquals("test-model", client.state.request.get().model());
        assertEquals("test-voice", client.state.request.get().voice());
        assertEquals("hello", client.state.request.get().text());

        handle.cancel();
        handle.terminal().toCompletableFuture().get(1, TimeUnit.SECONDS);

        assertEquals(1, client.state.cancels.get());
        assertEquals(0, listener.completed.get());
        assertEquals(0, listener.errors.get());
    }

    @Test
    void cancelAfterCompletionDoesNotTouchReturnedConnection() throws Exception {
        RecordingListener listener = new RecordingListener();
        WsAttemptHandle handle = client.startWsAttempt("hello", listener, target());

        client.state.providerListener.get().onComplete();
        handle.terminal().toCompletableFuture().get(1, TimeUnit.SECONDS);
        handle.cancel();

        assertEquals(0, client.state.cancels.get());
        assertEquals(1, listener.completed.get());
        assertEquals(0, listener.errors.get());
    }

    @Test
    void reusesCompletedConnectionForNextTaskOfSameModel() throws Exception {
        WsAttemptHandle first = client.startWsAttempt("first", new RecordingListener(), target());
        client.state.providerListener.get().onComplete();
        first.terminal().toCompletableFuture().get(1, TimeUnit.SECONDS);

        WsAttemptHandle second = client.startWsAttempt("second", new RecordingListener(), target());
        client.state.providerListener.get().onComplete();
        second.terminal().toCompletableFuture().get(1, TimeUnit.SECONDS);

        assertEquals(1, client.state.createdConnections.get());
    }

    private static final class TestTtsClient extends AbstractTtsClient<TestProviderRequest> {

        private final TestState state;

        private TestTtsClient() {
            this(new TestState());
        }

        private TestTtsClient(TestState state) {
            super(TestProviderRequest::new, ignored -> {
                state.createdConnections.incrementAndGet();
                return new TestConnection(state);
            });
            this.state = state;
        }

        @Override
        public String provider() {
            return "test";
        }
    }

    private static final class TestState {

        private final AtomicInteger cancels = new AtomicInteger();
        private final AtomicInteger createdConnections = new AtomicInteger();
        private final AtomicReference<TtsSynthesisRequest> request = new AtomicReference<>();
        private final AtomicReference<TtsStreamListener> providerListener = new AtomicReference<>();
        private final AtomicReference<String> streamThread = new AtomicReference<>();
    }

    private static final class TestConnection implements TtsConnection<TestProviderRequest> {

        private final TestState state;

        private TestConnection(TestState state) {
            this.state = state;
        }

        @Override
        public void stream(TestProviderRequest request, String text, TtsStreamListener listener) {
            state.streamThread.set(Thread.currentThread().getName());
            state.request.set(request.request());
            state.providerListener.set(listener);
        }

        @Override
        public void cancel() {
            state.cancels.incrementAndGet();
        }

        @Override
        public void close() {
        }
    }

    private record TestProviderRequest(TtsSynthesisRequest request) {
    }

    private static ModelTarget target() {
        AIModelProperties.ProviderConfig provider = new AIModelProperties.ProviderConfig();
        provider.setApiKey("test-api-key");
        provider.getTts().setVoice("test-voice");
        AIModelProperties.ModelCandidate candidate = new AIModelProperties.ModelCandidate();
        candidate.setId("test-tts");
        candidate.setProvider("test");
        candidate.setModel("test-model");
        return new ModelTarget("test-tts", candidate, provider, null);
    }

    private static final class RecordingListener implements TtsStreamListener {

        private final AtomicInteger completed = new AtomicInteger();
        private final AtomicInteger errors = new AtomicInteger();

        @Override
        public void onAudio(byte[] audio) {
        }

        @Override
        public void onComplete() {
            completed.incrementAndGet();
        }

        @Override
        public void onError(Throwable throwable) {
            errors.incrementAndGet();
        }
    }
}
