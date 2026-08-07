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

import com.nageoffer.ai.ragent.infra.config.AIModelProperties;
import com.nageoffer.ai.ragent.infra.model.ModelTarget;
import okhttp3.Request;
import okhttp3.WebSocketListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class WsTaskExecutorTest {

    private static final String MODEL_ID = "task-model";

    private TestConnection connection;
    private WsExecutor<TestConnection> connectionPool;
    private WsTaskExecutor<String, String, byte[], TestConnection> taskExecutor;

    @BeforeEach
    void setUp() {
        connectionPool = new WsExecutor<>(target -> {
            connection = new TestConnection(target);
            return connection;
        }, new WsExecutorConfig(1, 1));
        taskExecutor = new WsTaskExecutor<>(connectionPool, Runnable::run);
    }

    @AfterEach
    void tearDown() {
        taskExecutor.close();
    }

    @Test
    void sendsMultipleInputsAndReturnsConnectionAfterFinish() {
        WsTaskSession<String> session = taskExecutor.openTask(target(), "param", new NoopCallback());

        session.send("first");
        session.send("second");
        session.finish().toCompletableFuture().join();

        assertEquals(List.of("first", "second"), connection.inputs);
        assertFalse(session.isActive());
        assertEquals(1, connectionPool.getIdleCount(MODEL_ID));
        assertEquals(0, connectionPool.getActiveCount(MODEL_ID));
    }

    @Test
    void invalidatesConnectionWhenCancelFails() {
        WsTaskSession<String> session = taskExecutor.openTask(target(), "param", new NoopCallback());
        connection.failCancel = true;

        session.cancel();

        assertFalse(session.isActive());
        assertEquals(0, connectionPool.getIdleCount(MODEL_ID));
        assertEquals(1, connection.closeCount.get());
    }

    @Test
    void invalidatesConnectionWhenFailedTaskIsCancelledSuccessfully() {
        WsTaskSession<String> session = taskExecutor.openTask(target(), "param", new NoopCallback());

        session.cancelAndInvalidate();

        assertFalse(session.isActive());
        assertEquals(0, connectionPool.getIdleCount(MODEL_ID));
        assertEquals(1, connection.closeCount.get());
    }

    @Test
    void invalidatesConnectionWhenFinishCompletesWithoutAudio() {
        AtomicBoolean audioReceived = new AtomicBoolean();
        WsTaskSession<String> session = taskExecutor.openTask(
                target(),
                "param",
                new NoopCallback(),
                () -> !audioReceived.get()
        );

        session.finish().toCompletableFuture().join();

        assertFalse(session.isActive());
        assertEquals(0, connectionPool.getIdleCount(MODEL_ID));
        assertEquals(1, connection.closeCount.get());
    }

    private ModelTarget target() {
        AIModelProperties.ModelCandidate candidate = new AIModelProperties.ModelCandidate();
        candidate.setId(MODEL_ID);
        candidate.setProvider("test");
        candidate.setModel(MODEL_ID);
        return new ModelTarget(MODEL_ID, candidate, new AIModelProperties.ProviderConfig(), 1000L);
    }

    private static final class TestConnection extends VoiceConnection<String, String, byte[]> {

        private final List<String> inputs = new ArrayList<>();
        private final AtomicInteger closeCount = new AtomicInteger();
        private boolean failCancel;

        private TestConnection(ModelTarget target) {
            super(target, TestWebSocketFactory.INSTANCE);
        }

        @Override
        protected Request buildWebSocketRequest() {
            return new Request.Builder().url("ws://localhost/task-test").build();
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
            inputs.add(request);
        }

        @Override
        protected void doFinishTask(String taskId) {
            currentCallback().onComplete();
        }

        @Override
        protected void doCancelTask(String taskId) {
            if (failCancel) {
                throw new IllegalStateException("cancel failed");
            }
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
}
