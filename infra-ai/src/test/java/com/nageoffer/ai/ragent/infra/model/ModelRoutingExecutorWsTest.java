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

package com.nageoffer.ai.ragent.infra.model;

import com.nageoffer.ai.ragent.infra.config.AIModelProperties;
import com.nageoffer.ai.ragent.infra.enums.ModelCapability;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** WS 路由执行器测试。 */
class ModelRoutingExecutorWsTest {

    @Test
    void fallsBackWhenModelWsPoolIsFullWithoutCountingModelFailure() throws Exception {
        AIModelProperties properties = new AIModelProperties();
        properties.setProviders(Map.of("test", new AIModelProperties.ProviderConfig()));
        ModelHealthStore healthStore = new ModelHealthStore(properties);
        WsModelExecutorRegistry registry = new WsModelExecutorRegistry(1, 0);
        ModelRoutingExecutor executor = new ModelRoutingExecutor(healthStore, registry);
        ModelTarget occupiedTarget = target("model-a");
        ModelTarget fallbackTarget = target("model-b");
        CountDownLatch occupiedStarted = new CountDownLatch(1);
        CountDownLatch releaseOccupied = new CountDownLatch(1);
        AtomicInteger fallbackCalls = new AtomicInteger();

        try {
            WsRoutingTask<byte[]> occupied = executor.executeWsWithFallback(
                    ModelCapability.TTS,
                    List.of(occupiedTarget),
                    target -> target,
                    (client, target, sink) -> {
                        occupiedStarted.countDown();
                        releaseOccupied.await();
                        return handle();
                    },
                    packet -> packet.length > 0,
                    emptyObserver(),
                    new WsRoutingOptions(5_000, 10_000)
            );
            assertTrue(occupiedStarted.await(2, TimeUnit.SECONDS));

            WsRoutingTask<byte[]> fallback = executor.executeWsWithFallback(
                    ModelCapability.TTS,
                    List.of(occupiedTarget, fallbackTarget),
                    target -> target,
                    (client, target, sink) -> {
                        if (target == fallbackTarget) {
                            fallbackCalls.incrementAndGet();
                            sink.onEvent(new byte[]{1});
                        }
                        return handle();
                    },
                    packet -> packet.length > 0,
                    emptyObserver(),
                    new WsRoutingOptions(5_000, 10_000)
            );
            fallback.ready().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertEquals(1, fallbackCalls.get());
            assertTrue(healthStore.allowCall(occupiedTarget.id()));
            fallback.cancel();

            occupied.cancel();
            releaseOccupied.countDown();
        } finally {
            releaseOccupied.countDown();
            registry.destroy();
        }
    }

    @Test
    void waitsForRunningAttemptToFinishBeforeStartingFallback() throws Exception {
        AIModelProperties properties = new AIModelProperties();
        ModelHealthStore healthStore = new ModelHealthStore(properties);
        WsModelExecutorRegistry registry = new WsModelExecutorRegistry(1, 0);
        ModelRoutingExecutor executor = new ModelRoutingExecutor(healthStore, registry);
        CountDownLatch startEntered = new CountDownLatch(1);
        CountDownLatch releaseStart = new CountDownLatch(1);
        AtomicInteger fallbackCalls = new AtomicInteger();

        try {
            WsRoutingTask<byte[]> task = executor.executeWsWithFallback(
                    ModelCapability.TTS,
                    List.of(target("model-a", 30L), target("model-b")),
                    target -> target,
                    (client, target, sink) -> {
                        if ("model-a".equals(target.id())) {
                            startEntered.countDown();
                            releaseStart.await();
                            return handle();
                        }
                        fallbackCalls.incrementAndGet();
                        sink.onEvent(new byte[]{1});
                        return handle();
                    },
                    packet -> packet.length > 0,
                    emptyObserver(),
                    new WsRoutingOptions(30, 2_000)
            );

            assertTrue(startEntered.await(1, TimeUnit.SECONDS));
            Thread.sleep(100);
            assertEquals(0, fallbackCalls.get());

            releaseStart.countDown();
            task.ready().toCompletableFuture().get(1, TimeUnit.SECONDS);
            assertEquals(1, fallbackCalls.get());
            task.cancel();
        } finally {
            releaseStart.countDown();
            registry.destroy();
        }
    }

    @Test
    void ignoresLatePacketFromTimedOutAttempt() throws Exception {
        AIModelProperties properties = new AIModelProperties();
        ModelHealthStore healthStore = new ModelHealthStore(properties);
        WsModelExecutorRegistry registry = new WsModelExecutorRegistry(1, 0);
        ModelRoutingExecutor executor = new ModelRoutingExecutor(healthStore, registry);
        AtomicReference<WsAttemptSink<byte[]>> primarySink = new AtomicReference<>();
        CompletableFuture<Void> primaryTerminal = new CompletableFuture<>();
        CountDownLatch primaryStarted = new CountDownLatch(1);
        CountDownLatch downstreamReceived = new CountDownLatch(1);
        AtomicInteger downstreamPackets = new AtomicInteger();
        AtomicInteger fallbackCalls = new AtomicInteger();

        try {
            WsRoutingTask<byte[]> task = executor.executeWsWithFallback(
                    ModelCapability.TTS,
                    List.of(target("model-a", 30L), target("model-b")),
                    target -> target,
                    (client, target, sink) -> {
                        if ("model-a".equals(target.id())) {
                            primarySink.set(sink);
                            primaryStarted.countDown();
                            return controlledHandle(primaryTerminal);
                        }
                        fallbackCalls.incrementAndGet();
                        sink.onEvent(new byte[]{2});
                        return handle();
                    },
                    packet -> packet.length > 0,
                    new WsAttemptSink<>() {
                        @Override
                        public void onEvent(byte[] event) {
                            downstreamPackets.incrementAndGet();
                            downstreamReceived.countDown();
                        }

                        @Override
                        public void onComplete() {
                        }

                        @Override
                        public void onError(Throwable throwable) {
                        }
                    },
                    new WsRoutingOptions(30, 2_000)
            );

            assertTrue(primaryStarted.await(1, TimeUnit.SECONDS));
            Thread.sleep(100);
            primarySink.get().onEvent(new byte[]{9});
            assertEquals(0, downstreamPackets.get());
            assertEquals(0, fallbackCalls.get());

            primaryTerminal.complete(null);
            task.ready().toCompletableFuture().get(1, TimeUnit.SECONDS);
            assertTrue(downstreamReceived.await(1, TimeUnit.SECONDS));
            assertEquals(1, fallbackCalls.get());
            assertEquals(1, downstreamPackets.get());
            task.cancel();
        } finally {
            primaryTerminal.complete(null);
            registry.destroy();
        }
    }

    private static WsAttemptHandle handle() {
        CompletableFuture<Void> terminal = new CompletableFuture<>();
        return new WsAttemptHandle() {
            @Override
            public java.util.concurrent.CompletionStage<Void> terminal() {
                return terminal;
            }

            @Override
            public void cancel() {
                terminal.complete(null);
            }
        };
    }

    private static WsAttemptHandle controlledHandle(CompletableFuture<Void> terminal) {
        return new WsAttemptHandle() {
            @Override
            public java.util.concurrent.CompletionStage<Void> terminal() {
                return terminal;
            }

            @Override
            public void cancel() {
            }
        };
    }

    private static WsAttemptSink<byte[]> emptyObserver() {
        return new WsAttemptSink<>() {
            @Override
            public void onEvent(byte[] event) {
            }

            @Override
            public void onComplete() {
            }

            @Override
            public void onError(Throwable throwable) {
            }
        };
    }

    private static ModelTarget target(String id) {
        return target(id, 5_000L);
    }

    private static ModelTarget target(String id, long timeoutMs) {
        AIModelProperties.ModelCandidate candidate = new AIModelProperties.ModelCandidate();
        candidate.setId(id);
        candidate.setProvider("test");
        candidate.setModel(id);
        return new ModelTarget(id, candidate, new AIModelProperties.ProviderConfig(), timeoutMs);
    }
}
