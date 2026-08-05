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
import com.nageoffer.ai.ragent.infra.model.ModelHealthStore;
import com.nageoffer.ai.ragent.infra.model.ModelRoutingExecutor;
import com.nageoffer.ai.ragent.infra.model.ModelSelector;
import com.nageoffer.ai.ragent.infra.model.ModelTarget;
import com.nageoffer.ai.ragent.infra.model.WsModelExecutorRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TTS 首包探测、降级与熔断测试。
 */
class RoutingTtsServiceTest {

    @Test
    void fallsBackAfterFirstCandidateErrorWithoutLeakingFailedCallbacks() throws Exception {
        TestTtsClient failed = client("provider-a", listener -> {
            listener.onAudio(new byte[0]);
            listener.onError(new IllegalStateException("first failed"));
        });
        TestTtsClient success = client("provider-b", listener -> {
            listener.onAudio(new byte[]{1, 2, 3});
            listener.onComplete();
        });
        RecordingListener downstream = new RecordingListener();

        service(properties(1000L), failed, success).streamTts("hello", downstream);
        assertTrue(downstream.terminal.await(2, TimeUnit.SECONDS));

        assertEquals(1, failed.calls.get());
        assertEquals(1, failed.cancels.get());
        assertEquals(1, success.calls.get());
        assertEquals(0, downstream.errors.get());
        assertEquals(1, downstream.completes.get());
        assertArrayEquals(new byte[]{1, 2, 3}, downstream.audio());
    }

    @Test
    void fallsBackAndCancelsCandidateAfterFirstPacketTimeout() throws Exception {
        TestTtsClient timeout = client("provider-a", listener -> {
        });
        TestTtsClient success = client("provider-b", listener -> {
            listener.onAudio(new byte[]{4, 5});
            listener.onComplete();
        });
        RecordingListener downstream = new RecordingListener();

        service(properties(30L), timeout, success).streamTts("hello", downstream);

        assertTrue(downstream.terminal.await(2, TimeUnit.SECONDS));
        assertEquals(1, timeout.cancels.get());
        assertEquals(1, success.calls.get());
        assertArrayEquals(new byte[]{4, 5}, downstream.audio());
    }

    @Test
    void fallsBackWhenCandidateCompletesWithoutAudio() throws Exception {
        TestTtsClient empty = client("provider-a", TtsStreamListener::onComplete);
        TestTtsClient success = client("provider-b", listener -> listener.onAudio(new byte[]{6}));
        RecordingListener downstream = new RecordingListener();

        service(properties(1000L), empty, success).streamTts("hello", downstream);

        assertTrue(downstream.firstAudio.await(2, TimeUnit.SECONDS));
        assertEquals(1, empty.cancels.get());
        assertEquals(0, downstream.completes.get());
        assertArrayEquals(new byte[]{6}, downstream.audio());
    }

    @Test
    void opensCircuitAndSkipsFailedModelOnNextRequest() throws Exception {
        AIModelProperties properties = properties(1000L);
        properties.getSelection().setFailureThreshold(1);
        TestTtsClient failed = client("provider-a", listener -> listener.onError(new IllegalStateException("failed")));
        TestTtsClient success = client("provider-b", listener -> {
            listener.onAudio(new byte[]{7});
            listener.onComplete();
        });
        RoutingTtsService service = service(properties, failed, success);

        RecordingListener first = new RecordingListener();
        RecordingListener second = new RecordingListener();
        service.streamTts("first", first);
        service.streamTts("second", second);
        assertTrue(first.terminal.await(2, TimeUnit.SECONDS));
        assertTrue(second.terminal.await(2, TimeUnit.SECONDS));

        assertEquals(1, failed.calls.get());
        assertEquals(2, success.calls.get());
    }

    @Test
    void doesNotFallbackAfterFirstPacketHasBeenCommitted() throws Exception {
        AtomicReference<TtsStreamListener> primaryListener = new AtomicReference<>();
        TestTtsClient primary = client("provider-a", listener -> {
            primaryListener.set(listener);
            listener.onAudio(new byte[]{8});
        });
        TestTtsClient fallback = client("provider-b", listener -> listener.onAudio(new byte[]{9}));
        RecordingListener downstream = new RecordingListener();

        service(properties(1000L), primary, fallback).streamTts("hello", downstream);
        assertTrue(downstream.firstAudio.await(2, TimeUnit.SECONDS));
        primaryListener.get().onError(new IllegalStateException("stream interrupted"));
        assertTrue(downstream.terminal.await(2, TimeUnit.SECONDS));

        assertEquals(1, primary.calls.get());
        assertEquals(0, fallback.calls.get());
        assertEquals(1, downstream.errors.get());
        assertArrayEquals(new byte[]{8}, downstream.audio());
    }

    private RoutingTtsService service(AIModelProperties properties, TtsClient... clients) {
        ModelHealthStore healthStore = new ModelHealthStore(properties);
        ModelSelector selector = new ModelSelector(properties, healthStore);
        ModelRoutingExecutor executor = new ModelRoutingExecutor(
                healthStore,
                new WsModelExecutorRegistry(2, 0)
        );
        return new RoutingTtsService(selector, executor, List.of(clients));
    }

    private TestTtsClient client(String provider, StreamAction action) {
        return new TestTtsClient(provider, action);
    }

    private AIModelProperties properties(long firstTimeoutMs) {
        AIModelProperties properties = new AIModelProperties();
        properties.setProviders(Map.of(
                "provider-a", providerConfig(),
                "provider-b", providerConfig()
        ));

        AIModelProperties.ModelCandidate first = candidate("tts-a", "provider-a", 1, firstTimeoutMs);
        AIModelProperties.ModelCandidate second = candidate("tts-b", "provider-b", 2, 1000L);
        AIModelProperties.ModelGroup tts = new AIModelProperties.ModelGroup();
        tts.setDefaultModel("tts-a");
        tts.setCandidates(List.of(first, second));
        properties.setTts(tts);
        return properties;
    }

    private AIModelProperties.ProviderConfig providerConfig() {
        return new AIModelProperties.ProviderConfig();
    }

    private AIModelProperties.ModelCandidate candidate(String id,
                                                        String provider,
                                                        int priority,
                                                        long timeoutMs) {
        AIModelProperties.ModelCandidate candidate = new AIModelProperties.ModelCandidate();
        candidate.setId(id);
        candidate.setProvider(provider);
        candidate.setModel(id + "-model");
        candidate.setPriority(priority);
        candidate.setTimeoutMs(timeoutMs);
        return candidate;
    }

    @FunctionalInterface
    private interface StreamAction {

        void run(TtsStreamListener listener);
    }

    private static final class TestTtsClient implements TtsClient {

        private final String provider;
        private final StreamAction action;
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicInteger cancels = new AtomicInteger();

        private TestTtsClient(String provider, StreamAction action) {
            this.provider = provider;
            this.action = action;
        }

        @Override
        public String provider() {
            return provider;
        }

        @Override
        public com.nageoffer.ai.ragent.infra.model.WsAttemptHandle startWsAttempt(
                String text,
                TtsStreamListener listener,
                ModelTarget target) {
            calls.incrementAndGet();
            CompletableFuture<Void> terminal = new CompletableFuture<>();
            TtsStreamListener forwarding = new TtsStreamListener() {
                @Override
                public void onAudio(byte[] audio) {
                    listener.onAudio(audio);
                }

                @Override
                public void onComplete() {
                    listener.onComplete();
                    terminal.complete(null);
                }

                @Override
                public void onError(Throwable throwable) {
                    listener.onError(throwable);
                    terminal.completeExceptionally(throwable);
                }
            };
            action.run(forwarding);
            AtomicBoolean cancelled = new AtomicBoolean();
            return new com.nageoffer.ai.ragent.infra.model.WsAttemptHandle() {
                @Override
                public java.util.concurrent.CompletionStage<Void> terminal() {
                    return terminal;
                }

                @Override
                public void cancel() {
                    if (cancelled.compareAndSet(false, true)) {
                        cancels.incrementAndGet();
                        terminal.complete(null);
                    }
                }
            };
        }
    }

    private static final class RecordingListener implements TtsStreamListener {

        private final List<byte[]> packets = new ArrayList<>();
        private final AtomicInteger completes = new AtomicInteger();
        private final AtomicInteger errors = new AtomicInteger();
        private final CountDownLatch firstAudio = new CountDownLatch(1);
        private final CountDownLatch terminal = new CountDownLatch(1);

        @Override
        public void onAudio(byte[] audio) {
            packets.add(audio);
            firstAudio.countDown();
        }

        @Override
        public void onComplete() {
            completes.incrementAndGet();
            terminal.countDown();
        }

        @Override
        public void onError(Throwable throwable) {
            errors.incrementAndGet();
            terminal.countDown();
        }

        private byte[] audio() {
            int length = packets.stream().mapToInt(packet -> packet.length).sum();
            byte[] audio = new byte[length];
            int offset = 0;
            for (byte[] packet : packets) {
                System.arraycopy(packet, 0, audio, offset, packet.length);
                offset += packet.length;
            }
            return audio;
        }
    }
}
