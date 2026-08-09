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

package com.nageoffer.ai.ragent.infra.voice.websocket;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;

/**
 * 一次已启动的 WebSocket 任务，统一管理发送、结束、取消和租约释放
 */
@Slf4j
public final class WebSocketTaskSession<I> {

    private enum Lifecycle {
        ACTIVE,
        FINISHING,
        CANCELLING,
        RELEASED
    }

    private final String taskId;
    private final VoiceConnection<?, I, ?> connection;
    private final WebSocketConnectionLease<?> lease;
    private final Executor taskExecutor;
    private final BooleanSupplier invalidateOnFinish;
    private final CompletableFuture<Void> completion = new CompletableFuture<>();
    private final Object lifecycleLock = new Object();
    private Lifecycle lifecycle = Lifecycle.ACTIVE;

    <P, O, C extends VoiceConnection<P, I, O>> WebSocketTaskSession(String taskId,
                                                             C connection,
                                                             WebSocketConnectionLease<C> lease,
                                                             Executor taskExecutor,
                                                             BooleanSupplier invalidateOnFinish) {
        this.taskId = taskId;
        this.connection = connection;
        this.lease = lease;
        this.taskExecutor = taskExecutor;
        this.invalidateOnFinish = invalidateOnFinish;
    }

    public void send(I input) {
        synchronized (lifecycleLock) {
            if (lifecycle != Lifecycle.ACTIVE) {
                throw new IllegalStateException("WebSocket 任务已进入收尾阶段，taskId=" + taskId);
            }
            connection.send(taskId, input);
        }
    }

    public CompletionStage<Void> finish() {
        synchronized (lifecycleLock) {
            if (lifecycle != Lifecycle.ACTIVE) {
                return completion;
            }
            lifecycle = Lifecycle.FINISHING;
            try {
                taskExecutor.execute(this::finishTask);
            } catch (RuntimeException exception) {
                lease.invalidate();
                lifecycle = Lifecycle.RELEASED;
                lease.close();
                completion.completeExceptionally(exception);
            }
        }
        return completion;
    }

    public void cancel() {
        cancel(false);
    }

    public void cancelAndInvalidate() {
        cancel(true);
    }

    private void cancel(boolean invalidateConnection) {
        synchronized (lifecycleLock) {
            if (invalidateConnection) {
                lease.invalidate();
            }
            if (lifecycle == Lifecycle.RELEASED || lifecycle == Lifecycle.CANCELLING) {
                return;
            }
            lifecycle = Lifecycle.CANCELLING;
        }

        Throwable failure = null;
        try {
            connection.cancelTask(taskId);
        } catch (RuntimeException exception) {
            failure = exception;
            lease.invalidate();
            log.warn("WebSocket 任务取消失败，modelId={}，taskId={}", connection.modelId(), taskId, exception);
        } finally {
            synchronized (lifecycleLock) {
                lifecycle = Lifecycle.RELEASED;
            }
            lease.close();
            if (failure == null) {
                completion.complete(null);
            } else {
                completion.completeExceptionally(failure);
            }
        }
    }

    private void finishTask() {
        Throwable failure = null;
        try {
            connection.finishTask(taskId);
        } catch (RuntimeException exception) {
            failure = exception;
            lease.invalidate();
        }

        boolean release;
        synchronized (lifecycleLock) {
            release = lifecycle == Lifecycle.FINISHING;
            if (release) {
                lifecycle = Lifecycle.RELEASED;
            }
        }
        if (!release) {
            return;
        }

        if (failure == null && invalidateOnFinish.getAsBoolean()) {
            lease.invalidate();
        }
        lease.close();
        if (failure == null) {
            completion.complete(null);
        } else {
            completion.completeExceptionally(failure);
        }
    }
}
