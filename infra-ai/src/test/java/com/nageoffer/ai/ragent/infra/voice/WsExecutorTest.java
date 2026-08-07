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

package com.nageoffer.ai.ragent.infra.voice;

import com.nageoffer.ai.ragent.framework.exception.RemoteException;
import com.nageoffer.ai.ragent.infra.config.AIModelProperties;
import com.nageoffer.ai.ragent.infra.enums.ModelCapability;
import com.nageoffer.ai.ragent.infra.http.ModelClientException;
import com.nageoffer.ai.ragent.infra.model.ModelHealthStore;
import com.nageoffer.ai.ragent.infra.model.ModelRoutingExecutor;
import com.nageoffer.ai.ragent.infra.model.ModelTarget;
import okhttp3.Request;
import okhttp3.WebSocketListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WsExecutorTest {

    private static final String MODEL_ID = "voice-model";

    private TestConnectionFactory connectionFactory;
    private WsExecutor<TestVoiceConnection> executor;

    @BeforeEach
    void setUp() {
        connectionFactory = new TestConnectionFactory();
        executor = new WsExecutor<>(connectionFactory, new WsExecutorConfig(4, 4, 0));
    }

    @AfterEach
    void tearDown() {
        executor.close();
    }

    @Test
    void isolatesAndReusesConnectionsByModelId() {
        TestVoiceConnection modelAConnection;
        try (WsConnectionLease<TestVoiceConnection> lease = executor.acquire(target("model-a"))) {
            modelAConnection = lease.connection();
        }

        try (WsConnectionLease<TestVoiceConnection> lease = executor.acquire(target("model-a"))) {
            assertSame(modelAConnection, lease.connection());
        }

        try (WsConnectionLease<TestVoiceConnection> lease = executor.acquire(target("model-b"))) {
            assertNotSame(modelAConnection, lease.connection());
            assertEquals("model-b", lease.connection().modelId());
        }

        assertEquals(1, executor.getIdleCount("model-a"));
        assertEquals(1, executor.getIdleCount("model-b"));
    }

    @Test
    void connectionFailureIsPropagatedAndNextAcquireCanRetry() {
        connectionFactory.failConnections(MODEL_ID, 1);

        assertThrows(ModelClientException.class, () -> executor.acquire(target(MODEL_ID)));
        try (WsConnectionLease<TestVoiceConnection> lease = executor.acquire(target(MODEL_ID))) {
            assertEquals(VoiceConnectionState.IDLE, lease.connection().state());
        }
    }

    @Test
    void modelRoutingExecutorOwnsFailureCounting() {
        AIModelProperties properties = new AIModelProperties();
        ModelHealthStore healthStore = new ModelHealthStore(properties);
        ModelRoutingExecutor routingExecutor = new ModelRoutingExecutor(healthStore);
        ModelTarget target = target(MODEL_ID);
        connectionFactory.failConnections(MODEL_ID, 1, 2);

        assertRoutingFailure(routingExecutor, target);
        assertFalse(healthStore.isUnavailable(MODEL_ID));

        assertRoutingFailure(routingExecutor, target);
        assertTrue(healthStore.isUnavailable(MODEL_ID));
    }

    @Test
    void connectionFailureDoesNotCancelActiveTask() {
        connectionFactory.failConnections(MODEL_ID, 2);

        try (WsConnectionLease<TestVoiceConnection> activeLease = executor.acquire(target(MODEL_ID))) {
            TestVoiceConnection activeConnection = activeLease.connection();
            activeConnection.startTask("task-running", "param", new NoopCallback());

            assertThrows(ModelClientException.class, () -> executor.acquire(target(MODEL_ID)));
            assertEquals(VoiceConnectionState.TASK_RUNNING, activeConnection.state());
            assertEquals(0, activeConnection.closeCount());

            activeConnection.finishTask("task-running");
        }
    }

    @Test
    void poolExhaustionIsPropagatedToCaller() {
        executor.close();
        executor = new WsExecutor<>(connectionFactory, new WsExecutorConfig(1, 1, 0));

        try (WsConnectionLease<TestVoiceConnection> ignored = executor.acquire(target(MODEL_ID))) {
            assertThrows(RemoteException.class, () -> executor.acquire(target(MODEL_ID)));
        }
    }

    @Test
    void unfinishedTaskConnectionIsDestroyedInsteadOfReturned() {
        TestVoiceConnection connection;
        try (WsConnectionLease<TestVoiceConnection> lease = executor.acquire(target(MODEL_ID))) {
            connection = lease.connection();
            connection.startTask("task-running", "param", new NoopCallback());
        }

        assertEquals(1, connection.closeCount());
        assertEquals(0, executor.getIdleCount(MODEL_ID));
    }

    private void assertRoutingFailure(ModelRoutingExecutor routingExecutor, ModelTarget target) {
        assertThrows(RemoteException.class, () -> routingExecutor.executeWithFallback(
                ModelCapability.CHAT,
                List.of(target),
                ignored -> executor,
                (wsExecutor, currentTarget) -> {
                    try (WsConnectionLease<TestVoiceConnection> lease = wsExecutor.acquire(currentTarget)) {
                        return lease.connection();
                    }
                }
        ));
    }

    private static ModelTarget target(String modelId) {
        AIModelProperties.ModelCandidate candidate = new AIModelProperties.ModelCandidate();
        candidate.setId(modelId);
        candidate.setProvider("test");
        candidate.setModel(modelId);
        return new ModelTarget(modelId, candidate, new AIModelProperties.ProviderConfig(), 1000L);
    }

    private static final class NoopCallback extends VoiceStreamCallback<byte[]> {

        @Override
        protected void onValidPacket(byte[] packet) {
        }

        @Override
        protected void onTaskComplete() {
        }

        @Override
        protected void onTaskError(Throwable throwable) {
        }
    }

    private static final class TestConnectionFactory implements VoiceConnectionFactory<TestVoiceConnection> {

        private final Map<String, AtomicInteger> attemptsByModel = new ConcurrentHashMap<>();
        private final Map<String, List<Integer>> failedAttemptsByModel = new ConcurrentHashMap<>();

        @Override
        public TestVoiceConnection create(ModelTarget target) {
            String modelId = target.id();
            int attempt = attemptsByModel.computeIfAbsent(modelId, ignored -> new AtomicInteger())
                    .incrementAndGet();
            boolean failConnection = failedAttemptsByModel.getOrDefault(modelId, List.of()).contains(attempt);
            return new TestVoiceConnection(target, failConnection);
        }

        private void failConnections(String modelId, Integer... attempts) {
            failedAttemptsByModel.put(modelId, List.of(attempts));
        }
    }

    private static final class TestVoiceConnection extends VoiceConnection<String, String, byte[]> {

        private final boolean failConnection;
        private final AtomicInteger closeCount = new AtomicInteger();

        private TestVoiceConnection(ModelTarget target, boolean failConnection) {
            super(target, TestWebSocketFactory.INSTANCE);
            this.failConnection = failConnection;
        }

        private int closeCount() {
            return closeCount.get();
        }

        @Override
        protected Request buildWebSocketRequest() {
            if (failConnection) {
                throw new IllegalStateException("connect failed");
            }
            return new Request.Builder().url("ws://localhost/voice-test").build();
        }

        @Override
        protected WebSocketListener createWebSocketListener() {
            return new WebSocketListener() {
            };
        }

        @Override
        protected void awaitConnectionReady() {
        }

        @Override
        protected void doStartTask(String taskId, String param) {
        }

        @Override
        protected void awaitTaskStarted(String taskId) {
        }

        @Override
        protected void doSend(String taskId, String request) {
        }

        @Override
        protected void doFinishTask(String taskId) {
        }

        @Override
        protected void doCancelTask(String taskId) {
        }

        @Override
        protected void awaitTaskTerminated(String taskId) {
        }

        @Override
        protected void clearTaskContext() {
        }

        @Override
        protected void clearConnectionContext() {
            closeCount.incrementAndGet();
        }
    }
}
