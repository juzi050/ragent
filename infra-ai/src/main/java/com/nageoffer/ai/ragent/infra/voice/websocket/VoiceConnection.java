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

import com.nageoffer.ai.ragent.infra.http.ModelClientErrorType;
import com.nageoffer.ai.ragent.infra.http.ModelClientException;
import com.nageoffer.ai.ragent.infra.model.ModelTarget;
import okhttp3.Request;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 可复用 Voice WebSocket 连接模板，一条物理连接同时最多承载一个 taskId
 */
public abstract class VoiceConnection<P, I, O> implements AutoCloseable {

    private final ModelTarget target;
    private final WebSocket.Factory webSocketFactory;
    private final AtomicBoolean connectStarted = new AtomicBoolean();
    private final AtomicReference<VoiceConnectionState> state = new AtomicReference<>(VoiceConnectionState.CONNECTING);
    private final AtomicReference<String> currentTaskId = new AtomicReference<>();
    private final AtomicReference<VoiceStreamCallback<O>> currentCallback = new AtomicReference<>();
    private final AtomicBoolean taskErrorNotified = new AtomicBoolean();
    private final AtomicBoolean cancelling = new AtomicBoolean();
    private volatile WebSocket webSocket;

    protected VoiceConnection(ModelTarget target, WebSocket.Factory webSocketFactory) {
        this.target = target;
        this.webSocketFactory = webSocketFactory;
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
            awaitConnectionReady();
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
        try {
            doStartTask(taskId, param);
            awaitTaskStarted(taskId);
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
     * 取消当前任务，收到终态后连接变为空闲
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

    /**
     * 当前是否处于取消流程 供应商响应 task-failed 或连接关闭时用于区分预期取消
     */
    protected final boolean isCancelling() {
        return cancelling.get();
    }

    public final String modelId() {
        return target.id();
    }

    public final ModelTarget target() {
        return target;
    }

    public final VoiceConnectionState state() {
        return state.get();
    }

    public final String currentTaskId() {
        return currentTaskId.get();
    }

    public final boolean isReusable() {
        return state.get() == VoiceConnectionState.IDLE && currentTaskId.get() == null;
    }

    /**
     * 物理连接异常关闭时调用
     */
    protected final void connectionBroken(Throwable cause) {
        if (markBroken()) {
            notifyTaskError(cause);
        }
    }

    protected final VoiceStreamCallback<O> currentCallback() {
        return currentCallback.get();
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
            clearConnectionContext();
        }
    }

    /**
     * 构建供应商连接请求，URL、鉴权头由具体客户端提供
     */
    protected abstract Request buildWebSocketRequest() throws Exception;

    /**
     * 创建供应商协议监听器
     */
    protected abstract WebSocketListener createWebSocketListener();

    protected final WebSocket webSocket() {
        return webSocket;
    }

    protected abstract void awaitConnectionReady() throws Exception;

    protected abstract void doStartTask(String taskId, P param) throws Exception;

    protected abstract void awaitTaskStarted(String taskId) throws Exception;

    protected abstract void doSend(String taskId, I request) throws Exception;

    protected abstract void doFinishTask(String taskId) throws Exception;

    protected abstract void doCancelTask(String taskId) throws Exception;

    protected abstract void awaitTaskTerminated(String taskId) throws Exception;

    /**
     * 清理当前任务的 Future 和协议上下文
     */
    protected abstract void clearTaskContext();

    /**
     * 清理连接级引用的可选钩子
     */
    protected void clearConnectionContext() {
    }

    private void openWebSocket() throws Exception {
        Request request = buildWebSocketRequest();
        WebSocketListener listener = createWebSocketListener();
        webSocket = webSocketFactory.newWebSocket(request, listener);
    }

    private void closeWebSocket() {
        WebSocket current = webSocket;
        if (current != null && !current.close(1000, "normal")) {
            current.cancel();
        }
    }

    protected final boolean markBroken() {
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
        // 幂等 与取消竞态时后到者静默返回 连接归还由先到者负责
        if (!Objects.equals(taskId, currentTaskId.get())) {
            return;
        }
        if (!state.compareAndSet(terminalState, VoiceConnectionState.IDLE)) {
            return;
        }
        resetTaskContext();
    }

    private void resetTaskContext() {
        clearTaskContext();
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

    private RuntimeException wrap(String message, Exception exception) {
        if (exception instanceof ModelClientException modelClientException) {
            return modelClientException;
        }
        return new ModelClientException(message, ModelClientErrorType.NETWORK_ERROR, null, exception);
    }
}
