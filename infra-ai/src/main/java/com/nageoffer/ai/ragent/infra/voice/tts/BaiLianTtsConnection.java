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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.nageoffer.ai.ragent.infra.config.AIModelProperties;
import com.nageoffer.ai.ragent.infra.enums.ModelCapability;
import com.nageoffer.ai.ragent.infra.http.HttpResponseHelper;
import com.nageoffer.ai.ragent.infra.http.ModelClientErrorType;
import com.nageoffer.ai.ragent.infra.http.ModelClientException;
import com.nageoffer.ai.ragent.infra.http.ModelUrlResolver;
import com.nageoffer.ai.ragent.infra.model.ModelTarget;
import com.nageoffer.ai.ragent.infra.voice.VoiceConnection;
import com.nageoffer.ai.ragent.infra.voice.VoiceConnectionState;
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

/**
 * CosyVoice WebSocket 物理连接
 */
final class BaiLianTtsConnection extends VoiceConnection<BaiLianTtsTaskParam, String, byte[]> {

    private final AIModelProperties.WebSocketConfig websocketConfig;
    private final Gson gson = new Gson();
    private final CompletableFuture<Void> connectionReady = new CompletableFuture<>();

    private volatile CompletableFuture<Void> taskStarted;
    private volatile CompletableFuture<Void> taskFinished;

    BaiLianTtsConnection(ModelTarget target,
                         WebSocket.Factory webSocketFactory,
                         AIModelProperties.WebSocketConfig websocketConfig) {
        super(target, webSocketFactory);
        this.websocketConfig = websocketConfig;
    }

    @Override
    protected Request buildWebSocketRequest() {
        AIModelProperties.ProviderConfig provider = HttpResponseHelper.requireProvider(target(), "bailian TTS");
        HttpResponseHelper.requireApiKey(provider, "bailian TTS");
        String url = toWebSocketUrl(ModelUrlResolver.resolveUrl(provider, target().candidate(), ModelCapability.TTS));
        Request.Builder request = new Request.Builder()
                .url(url)
                .addHeader("Authorization", "Bearer " + provider.getApiKey());
        if (provider.getWorkspace() != null && !provider.getWorkspace().isBlank()) {
            request.addHeader("X-DashScope-WorkSpace", provider.getWorkspace());
        }
        return request.build();
    }

    @Override
    protected WebSocketListener createWebSocketListener() {
        return new Listener();
    }

    @Override
    protected void awaitConnectionReady() throws Exception {
        await(connectionReady, websocketConfig.getConnectTimeoutMs());
    }

    @Override
    protected void doStartTask(String taskId, BaiLianTtsTaskParam param) {
        taskStarted = new CompletableFuture<>();
        taskFinished = new CompletableFuture<>();

        JsonObject parameters = new JsonObject();
        parameters.addProperty("text_type", "PlainText");
        parameters.addProperty("voice", param.voice());
        parameters.addProperty("format", param.audioFormat());

        JsonObject payload = new JsonObject();
        payload.addProperty("task", "tts");
        payload.addProperty("function", "SpeechSynthesizer");
        payload.addProperty("model", param.model());
        payload.add("parameters", parameters);
        payload.add("input", new JsonObject());

        sendJson(command("run-task", taskId, payload));
    }

    @Override
    protected void awaitTaskStarted(String taskId) throws Exception {
        await(taskStarted, websocketConfig.getTaskStartTimeoutMs());
    }

    @Override
    protected void doSend(String taskId, String text) {
        JsonObject input = new JsonObject();
        input.addProperty("text", text);
        JsonObject payload = new JsonObject();
        payload.add("input", input);
        sendJson(command("continue-task", taskId, payload));
    }

    @Override
    protected void doFinishTask(String taskId) {
        JsonObject payload = new JsonObject();
        payload.add("input", new JsonObject());
        sendJson(command("finish-task", taskId, payload));
    }

    @Override
    protected void doCancelTask(String taskId) {
        JsonObject input = new JsonObject();
        input.addProperty("directive", "cancel");
        JsonObject payload = new JsonObject();
        payload.add("input", input);
        sendJson(command("finish-task", taskId, payload));
    }

    @Override
    protected void awaitTaskTerminated(String taskId) throws Exception {
        await(taskFinished, websocketConfig.getTaskFinishTimeoutMs());
    }

    @Override
    protected void clearTaskContext() {
        taskStarted = null;
        taskFinished = null;
    }

    private JsonObject command(String action, String taskId, JsonObject payload) {
        JsonObject header = new JsonObject();
        header.addProperty("action", action);
        header.addProperty("task_id", taskId);
        header.addProperty("streaming", "duplex");

        JsonObject command = new JsonObject();
        command.add("header", header);
        command.add("payload", payload);
        return command;
    }

    private void sendJson(JsonObject message) {
        WebSocket current = webSocket();
        if (current == null || !current.send(gson.toJson(message))) {
            throw new ModelClientException("bailian TTS WebSocket 发送失败",
                    ModelClientErrorType.NETWORK_ERROR, null);
        }
    }

    private void handleTextMessage(String text) {
        JsonObject response = gson.fromJson(text, JsonObject.class);
        JsonObject header = response.getAsJsonObject("header");
        if (header == null || !header.has("event")) {
            return;
        }
        validateTaskId(header);
        String event = header.get("event").getAsString();
        switch (event) {
            case "task-started" -> taskStarted.complete(null);
            case "task-finished" -> {
                currentCallback().onComplete();
                taskFinished.complete(null);
            }
            case "task-failed" -> failTask(header);
            default -> {
                // result-generated 等元信息在此消费，音频数据经二进制帧进入统一回调
            }
        }
    }

    private void validateTaskId(JsonObject header) {
        String responseTaskId = header.has("task_id") ? header.get("task_id").getAsString() : null;
        if (!Objects.equals(currentTaskId(), responseTaskId)) {
            throw new ModelClientException(
                    "bailian TTS 响应 taskId 不一致，expected=" + currentTaskId() + "，actual=" + responseTaskId,
                    ModelClientErrorType.INVALID_RESPONSE,
                    null
            );
        }
    }

    private void failTask(JsonObject header) {
        String errorCode = header.has("error_code") ? header.get("error_code").getAsString() : "unknown";
        String errorMessage = header.has("error_message") ? header.get("error_message").getAsString() : "unknown";
        ModelClientException exception = new ModelClientException(
                "bailian TTS 任务失败: " + errorCode + " - " + errorMessage,
                ModelClientErrorType.PROVIDER_ERROR,
                null
        );
        CompletableFuture<Void> started = taskStarted;
        if (started != null) {
            started.completeExceptionally(exception);
        }
        CompletableFuture<Void> finished = taskFinished;
        if (finished != null) {
            finished.completeExceptionally(exception);
        }
        connectionBroken(exception);
    }

    private void failConnection(Throwable throwable) {
        connectionReady.completeExceptionally(throwable);
        CompletableFuture<Void> started = taskStarted;
        if (started != null) {
            started.completeExceptionally(throwable);
        }
        CompletableFuture<Void> finished = taskFinished;
        if (finished != null) {
            finished.completeExceptionally(throwable);
        }
        connectionBroken(throwable);
    }

    private void await(CompletableFuture<Void> future, long timeoutMs) throws Exception {
        try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException exception) {
            throwCause(exception);
        } catch (TimeoutException exception) {
            throw exception;
        }
    }

    private void throwCause(ExecutionException exception) throws Exception {
        Throwable cause = exception.getCause();
        if (cause instanceof Exception checkedException) {
            throw checkedException;
        }
        throw new RuntimeException(cause);
    }

    private String toWebSocketUrl(String url) {
        if (url.startsWith("https://")) {
            return "wss://" + url.substring("https://".length());
        }
        if (url.startsWith("http://")) {
            return "ws://" + url.substring("http://".length());
        }
        return url;
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
            if (currentCallback() != null) {
                currentCallback().onPacket(bytes.toByteArray());
            }
        }

        @Override
        public void onClosed(WebSocket webSocket, int code, String reason) {
            if (state() != VoiceConnectionState.CLOSED) {
                failConnection(new ModelClientException(
                        "bailian TTS WebSocket 已关闭: " + code + " - " + reason,
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
}
