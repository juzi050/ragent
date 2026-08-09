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

import com.nageoffer.ai.ragent.infra.config.AIModelProperties;
import com.nageoffer.ai.ragent.infra.http.ModelClientErrorType;
import com.nageoffer.ai.ragent.infra.http.ModelClientException;
import com.nageoffer.ai.ragent.infra.model.ModelTarget;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 可复用 Voice WebSocket 连接模板，一条物理连接同时最多承载一个 taskId
 */
public abstract class VoiceConnection<P, I, O> implements AutoCloseable {

    private final ModelTarget target;
    private final WebSocket.Factory webSocketFactory;
    private final AIModelProperties.WebSocketConfig webSocketConfig;
    private final AtomicBoolean connectStarted = new AtomicBoolean();
    private final AtomicReference<VoiceConnectionState> state = new AtomicReference<>(VoiceConnectionState.CONNECTING);
    private final AtomicReference<String> currentTaskId = new AtomicReference<>();
    private final AtomicReference<VoiceStreamCallback<O>> currentCallback = new AtomicReference<>();
    private final AtomicBoolean taskErrorNotified = new AtomicBoolean();
    private final AtomicBoolean cancelling = new AtomicBoolean();
    private final CompletableFuture<Void> connectionReady = new CompletableFuture<>();
    private volatile CompletableFuture<Void> taskStarted;
    private volatile CompletableFuture<Void> taskFinished;
    private volatile long lastPacketReceivedMs;
    private volatile WebSocket webSocket;

    protected VoiceConnection(ModelTarget target,
                              WebSocket.Factory webSocketFactory,
                              AIModelProperties.WebSocketConfig webSocketConfig) {
        this.target = target;
        this.webSocketFactory = webSocketFactory;
        this.webSocketConfig = webSocketConfig;
    }

    /**
     * 建立 WebSocket 并完成连接级初始化
     */
    public final void connect() {
        if (!connectStarted.compareAndSet(false, true)) {
            throw new IllegalStateException("Voice 连接只能建立一次，modelId=" + modelId());
        }
        try {
            openWebSocket();
            await(connectionReady, webSocketConfig.getConnectTimeoutMs());
            if (!state.compareAndSet(VoiceConnectionState.CONNECTING, VoiceConnectionState.IDLE)) {
                throw new IllegalStateException("Voice 连接在初始化期间失效，modelId=" + modelId());
            }
        } catch (Exception exception) {
            markBroken();
            throw wrap("Voice WebSocket 建连失败，modelId=" + modelId(), exception);
        }
    }

    /**
     * 在当前空闲连接上启动一次任务
     */
    public final void startTask(String taskId, P param, VoiceStreamCallback<O> callback) {
        if (!state.compareAndSet(VoiceConnectionState.IDLE, VoiceConnectionState.TASK_STARTING)) {
            throw new IllegalStateException("Voice 连接当前不可启动任务，modelId=" + modelId() + "，state=" + state.get());
        }
        currentTaskId.set(taskId);
        currentCallback.set(callback);
        taskErrorNotified.set(false);
        taskStarted = new CompletableFuture<>();
        taskFinished = new CompletableFuture<>();
        lastPacketReceivedMs = 0L;
        try {
            doStartTask(taskId, param);
            await(taskStarted, webSocketConfig.getTaskStartTimeoutMs());
            if (!state.compareAndSet(VoiceConnectionState.TASK_STARTING, VoiceConnectionState.TASK_RUNNING)) {
                throw new IllegalStateException("Voice 任务启动期间连接失效，modelId=" + modelId() + "，taskId=" + taskId);
            }
        } catch (Exception exception) {
            RuntimeException failure = wrap("Voice 任务启动失败，modelId=" + modelId() + "，taskId=" + taskId,
                    exception);
            markBrokenAndNotify(failure);
            throw failure;
        }
    }

    /**
     * 向当前任务持续发送内容
     */
    public final void send(String taskId, I request) {
        requireCurrentTask(taskId, VoiceConnectionState.TASK_RUNNING);
        try {
            doSend(taskId, request);
        } catch (Exception exception) {
            RuntimeException failure = wrap("Voice 任务数据发送失败，modelId=" + modelId() + "，taskId=" + taskId,
                    exception);
            markBrokenAndNotify(failure);
            throw failure;
        }
    }

    /**
     * 正常结束当前任务，收到终态后连接变为空闲
     */
    public final void finishTask(String taskId) {
        requireCurrentTask(taskId, VoiceConnectionState.TASK_RUNNING);
        if (!state.compareAndSet(VoiceConnectionState.TASK_RUNNING, VoiceConnectionState.TASK_FINISHING)) {
            throw new IllegalStateException("Voice 任务当前不可结束，modelId=" + modelId() + "，taskId=" + taskId
                    + "，state=" + state.get());
        }
        try {
            doFinishTask(taskId);
            awaitTaskTerminated(taskId);
            completeTask(taskId, VoiceConnectionState.TASK_FINISHING);
        } catch (Exception exception) {
            RuntimeException failure = wrap("Voice 任务结束失败，modelId=" + modelId() + "，taskId=" + taskId,
                    exception);
            markBrokenAndNotify(failure);
            throw failure;
        }
    }

    /**
     * 取消当前任务，收到终态后将连接标记为不可复用
     */
    public final void cancelTask(String taskId) {
        if (state.get() == VoiceConnectionState.IDLE && currentTaskId.get() == null) {
            return;
        }
        requireCurrentTask(taskId, VoiceConnectionState.TASK_RUNNING, VoiceConnectionState.TASK_FINISHING,
                VoiceConnectionState.TASK_CANCELLING);
        VoiceConnectionState previous = state.getAndUpdate(current -> switch (current) {
            case TASK_RUNNING, TASK_FINISHING -> VoiceConnectionState.TASK_CANCELLING;
            default -> current;
        });
        if (previous != VoiceConnectionState.TASK_RUNNING
                && previous != VoiceConnectionState.TASK_FINISHING
                && previous != VoiceConnectionState.TASK_CANCELLING) {
            throw new IllegalStateException("Voice 任务当前不可取消，modelId=" + modelId() + "，taskId=" + taskId
                    + "，state=" + previous);
        }
        cancelling.set(true);
        try {
            if (previous != VoiceConnectionState.TASK_CANCELLING) {
                doCancelTask(taskId);
            }
            awaitTaskTerminated(taskId);
            completeTask(taskId, VoiceConnectionState.TASK_CANCELLING);
        } catch (Exception exception) {
            markBroken();
            throw wrap("Voice 任务取消失败，modelId=" + modelId() + "，taskId=" + taskId, exception);
        } finally {
            cancelling.set(false);
        }
    }

    public final String modelId() {
        return target.id();
    }

    public final ModelTarget target() {
        return target;
    }

    public final boolean isReusable() {
        return state.get() == VoiceConnectionState.IDLE && currentTaskId.get() == null;
    }

    /**
     * 物理连接异常关闭时调用
     */
    private void connectionBroken(Throwable cause) {
        if (markBroken()) {
            notifyTaskError(cause);
        }
    }

    @Override
    public final void close() {
        VoiceConnectionState previous = state.getAndSet(VoiceConnectionState.CLOSED);
        if (previous == VoiceConnectionState.CLOSED) {
            return;
        }
        resetTaskContext();
        try {
            closeWebSocket();
        } catch (Exception exception) {
            throw wrap("Voice WebSocket 关闭失败，modelId=" + modelId(), exception);
        } finally {
            webSocket = null;
        }
    }

    /**
     * 构建供应商连接请求，URL、鉴权头由具体客户端提供
     */
    protected abstract Request buildWebSocketRequest() throws Exception;

    protected final WebSocket webSocket() {
        return webSocket;
    }

    protected final String toWebSocketUrl(String url) {
        if (url.startsWith("https://")) {
            return "wss://" + url.substring("https://".length());
        }
        if (url.startsWith("http://")) {
            return "ws://" + url.substring("http://".length());
        }
        return url;
    }

    protected abstract void doStartTask(String taskId, P param) throws Exception;

    protected abstract void doSend(String taskId, I request) throws Exception;

    protected abstract void doFinishTask(String taskId) throws Exception;

    protected abstract void doCancelTask(String taskId) throws Exception;

    /**
     * 处理供应商文本帧
     */
    protected abstract void handleTextMessage(String text);

    /**
     * 将供应商二进制帧转换为业务数据包
     */
    protected abstract O decodeBinaryMessage(ByteString bytes);

    private void openWebSocket() throws Exception {
        Request request = buildWebSocketRequest();
        webSocket = webSocketFactory.newWebSocket(request, new Listener());
    }

    private void closeWebSocket() {
        WebSocket current = webSocket;
        if (current != null && !current.close(1000, "normal")) {
            current.cancel();
        }
    }

    /**
     * 校验供应商事件是否属于当前任务
     */
    protected final void validateResponseTaskId(String responseTaskId) {
        String expectedTaskId = currentTaskId.get();
        if (!Objects.equals(expectedTaskId, responseTaskId)) {
            throw new ModelClientException(
                    "Voice WebSocket 响应 taskId 不一致，expected=" + expectedTaskId + "，actual=" + responseTaskId,
                    ModelClientErrorType.INVALID_RESPONSE,
                    null
            );
        }
    }

    /**
     * 标记供应商任务已启动
     */
    protected final void markTaskStarted() {
        CompletableFuture<Void> started = taskStarted;
        if (started != null) {
            started.complete(null);
        }
    }

    /**
     * 标记供应商任务已正常结束
     */
    protected final void markTaskFinished() {
        VoiceStreamCallback<O> callback = currentCallback.get();
        if (callback != null) {
            callback.onComplete();
        }
        completeTaskFinished(null);
    }

    /**
     * 标记供应商任务失败
     */
    protected final void markTaskFailed(Throwable throwable) {
        if (cancelling.get()) {
            // 取消响应可能表现为 task-failed，不再上报业务错误，但连接不可复用
            markBroken();
            completeTaskFinished(null);
            return;
        }
        CompletableFuture<Void> started = taskStarted;
        if (started != null) {
            started.completeExceptionally(throwable);
        }
        completeTaskFinished(throwable);
        connectionBroken(throwable);
    }

    private void failConnection(Throwable throwable) {
        if (cancelling.get()) {
            // 取消过程中连接关闭属于预期，不再上报业务错误
            markBroken();
            completeTaskFinished(null);
            return;
        }
        connectionReady.completeExceptionally(throwable);
        CompletableFuture<Void> started = taskStarted;
        if (started != null) {
            started.completeExceptionally(throwable);
        }
        completeTaskFinished(throwable);
        connectionBroken(throwable);
    }

    private void completeTaskFinished(Throwable throwable) {
        CompletableFuture<Void> finished = taskFinished;
        if (finished == null) {
            return;
        }
        if (throwable == null) {
            finished.complete(null);
        } else {
            finished.completeExceptionally(throwable);
        }
    }

    private void awaitTaskTerminated(String taskId) throws Exception {
        CompletableFuture<Void> finished = taskFinished;
        long deadline = System.currentTimeMillis() + webSocketConfig.getTaskFinishTimeoutMs();
        while (!finished.isDone()) {
            long last = lastPacketReceivedMs;
            if (last > 0) {
                deadline = Math.max(deadline, last + webSocketConfig.getTaskFinishTimeoutMs());
            }
            if (System.currentTimeMillis() > deadline) {
                throw new TimeoutException("finish-task 后帧间空闲超时，taskId=" + taskId);
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            }
        }
        await(finished, webSocketConfig.getTaskFinishTimeoutMs());
    }

    private void await(CompletableFuture<Void> future, long timeoutMs) throws Exception {
        try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof Exception checkedException) {
                throw checkedException;
            }
            throw new RuntimeException(cause);
        }
    }

    private boolean markBroken() {
        while (true) {
            VoiceConnectionState current = state.get();
            if (current == VoiceConnectionState.CLOSED) {
                return false;
            }
            if (current == VoiceConnectionState.BROKEN
                    || state.compareAndSet(current, VoiceConnectionState.BROKEN)) {
                return true;
            }
        }
    }

    private void markBrokenAndNotify(Throwable throwable) {
        if (markBroken()) {
            notifyTaskError(throwable);
        }
    }

    private void notifyTaskError(Throwable throwable) {
        VoiceStreamCallback<O> callback = currentCallback.get();
        if (callback != null && taskErrorNotified.compareAndSet(false, true)) {
            callback.onError(throwable);
        }
    }

    private void completeTask(String taskId, VoiceConnectionState terminalState) {
        // 幂等 与取消竞态时后到者静默返回 租约释放由先到者负责
        if (!Objects.equals(taskId, currentTaskId.get())) {
            return;
        }
        VoiceConnectionState completedState = terminalState == VoiceConnectionState.TASK_CANCELLING
                ? VoiceConnectionState.BROKEN
                : VoiceConnectionState.IDLE;
        if (!state.compareAndSet(terminalState, completedState)) {
            return;
        }
        resetTaskContext();
    }

    private void resetTaskContext() {
        taskStarted = null;
        taskFinished = null;
        lastPacketReceivedMs = 0L;
        currentTaskId.set(null);
        currentCallback.set(null);
        taskErrorNotified.set(false);
    }

    private void requireCurrentTask(String taskId, VoiceConnectionState... allowedStates) {
        if (!Objects.equals(taskId, currentTaskId.get())) {
            throw new IllegalStateException("Voice taskId 与当前任务不一致，modelId=" + modelId()
                    + "，taskId=" + taskId + "，currentTaskId=" + currentTaskId.get());
        }
        VoiceConnectionState currentState = state.get();
        for (VoiceConnectionState allowedState : allowedStates) {
            if (currentState == allowedState) {
                return;
            }
        }
        throw new IllegalStateException("Voice 任务状态不允许当前操作，modelId=" + modelId() + "，taskId=" + taskId
                + "，state=" + currentState);
    }

    private final class Listener extends WebSocketListener {

        @Override
        public void onOpen(WebSocket webSocket, Response response) {
            connectionReady.complete(null);
        }

        @Override
        public void onMessage(WebSocket webSocket, String text) {
            try {
                handleTextMessage(text);
            } catch (RuntimeException exception) {
                failConnection(exception);
            }
        }

        @Override
        public void onMessage(WebSocket webSocket, ByteString bytes) {
            try {
                lastPacketReceivedMs = System.currentTimeMillis();
                O packet = decodeBinaryMessage(bytes);
                VoiceStreamCallback<O> callback = currentCallback.get();
                if (callback != null) {
                    callback.onPacket(packet);
                }
            } catch (RuntimeException exception) {
                failConnection(exception);
            }
        }

        @Override
        public void onClosed(WebSocket webSocket, int code, String reason) {
            if (state.get() != VoiceConnectionState.CLOSED) {
                failConnection(new ModelClientException(
                        "Voice WebSocket 已关闭: " + code + " - " + reason,
                        ModelClientErrorType.NETWORK_ERROR,
                        null
                ));
            }
        }

        @Override
        public void onFailure(WebSocket webSocket, Throwable throwable, Response response) {
            failConnection(throwable);
        }
    }

    private RuntimeException wrap(String message, Exception exception) {
        if (exception instanceof ModelClientException modelClientException) {
            return modelClientException;
        }
        return new ModelClientException(message, ModelClientErrorType.NETWORK_ERROR, null, exception);
    }
}
