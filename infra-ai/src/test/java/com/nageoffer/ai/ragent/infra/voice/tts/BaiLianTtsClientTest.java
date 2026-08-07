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
import com.nageoffer.ai.ragent.infra.http.ModelClientException;
import com.nageoffer.ai.ragent.infra.model.ModelTarget;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BaiLianTtsClientTest {

    private final Gson gson = new Gson();

    @Test
    void sendsMinimalCosyVoiceProtocolAndReturnsOpus() {
        FakeWebSocketFactory factory = new FakeWebSocketFactory();
        BaiLianTtsClient client = new BaiLianTtsClient(factory, Runnable::run, websocketConfig(), "opus");
        RecordingCallback callback = new RecordingCallback();

        TtsTask task = client.synthesize(
                new TtsRequest("你好，世界。", "longxiaochun"),
                callback,
                target()
        );

        JsonObject runTask = factory.messages.get(0);
        JsonObject runPayload = runTask.getAsJsonObject("payload");
        JsonObject parameters = runPayload.getAsJsonObject("parameters");
        assertEquals(Set.of("text_type", "voice", "format"), parameters.keySet());
        assertEquals("PlainText", parameters.get("text_type").getAsString());
        assertEquals("longxiaochun", parameters.get("voice").getAsString());
        assertEquals("opus", parameters.get("format").getAsString());
        assertEquals("cosyvoice-v3-flash", runPayload.get("model").getAsString());

        assertEquals(List.of("run-task", "continue-task", "finish-task"), factory.actions());
        assertEquals(1, factory.taskIds().stream().distinct().count());
        assertEquals("你好，世界。", factory.messages.get(1)
                .getAsJsonObject("payload")
                .getAsJsonObject("input")
                .get("text")
                .getAsString());
        assertEquals("Bearer test-key", factory.request.header("Authorization"));
        assertEquals("test-workspace", factory.request.header("X-DashScope-WorkSpace"));
        assertTrue(factory.request.url().isHttps());
        assertEquals("dashscope.aliyuncs.com", factory.request.url().host());
        assertEquals("/api-ws/v1/inference", factory.request.url().encodedPath());
        assertArrayEquals(FakeWebSocketFactory.OPUS_AUDIO, callback.audio);
        assertTrue(callback.completed);

        task.cancel();
        assertEquals(0, factory.closeCount.get());
        client.close();
    }

    @Test
    void rejectsControlEventWithDifferentTaskId() {
        FakeWebSocketFactory factory = new FakeWebSocketFactory();
        factory.mismatchedTaskId = true;
        BaiLianTtsClient client = new BaiLianTtsClient(factory, Runnable::run, websocketConfig(), "opus");

        assertThrows(ModelClientException.class, () -> client.synthesize(
                new TtsRequest("你好", "longxiaochun"),
                new RecordingCallback(),
                target()
        ));

        client.close();
    }

    private ModelTarget target() {
        AIModelProperties.ModelCandidate candidate = new AIModelProperties.ModelCandidate();
        candidate.setId("cosyvoice-v3-flash");
        candidate.setProvider("bailian");
        candidate.setModel("cosyvoice-v3-flash");

        AIModelProperties.ProviderConfig provider = new AIModelProperties.ProviderConfig();
        provider.setUrl("https://dashscope.aliyuncs.com");
        provider.setApiKey("test-key");
        provider.setWorkspace("test-workspace");
        provider.setEndpoints(java.util.Map.of("tts", "/api-ws/v1/inference"));
        return new ModelTarget(candidate.getId(), candidate, provider, 1000L);
    }

    private AIModelProperties.WebSocketConfig websocketConfig() {
        AIModelProperties.WebSocketConfig config = new AIModelProperties.WebSocketConfig();
        config.setConnectTimeoutMs(1000L);
        config.setTaskStartTimeoutMs(1000L);
        config.setTaskFinishTimeoutMs(1000L);
        config.setMaxTotalPerModel(1);
        config.setMaxIdlePerModel(1);
        return config;
    }

    private static final class RecordingCallback implements TtsCallback {

        private byte[] audio;
        private boolean completed;

        @Override
        public void onAudio(byte[] opusAudio) {
            audio = opusAudio;
        }

        @Override
        public void onComplete() {
            completed = true;
        }

        @Override
        public void onError(Throwable throwable) {
        }
    }

    private final class FakeWebSocketFactory implements WebSocket.Factory {

        private static final byte[] OPUS_AUDIO = {79, 112, 117, 115};

        private final List<JsonObject> messages = new ArrayList<>();
        private final AtomicInteger closeCount = new AtomicInteger();
        private Request request;
        private boolean mismatchedTaskId;

        @Override
        public WebSocket newWebSocket(Request request, WebSocketListener listener) {
            this.request = request;
            FakeWebSocket webSocket = new FakeWebSocket(request, listener);
            Response response = new Response.Builder()
                    .request(request)
                    .protocol(Protocol.HTTP_1_1)
                    .code(101)
                    .message("Switching Protocols")
                    .build();
            listener.onOpen(webSocket, response);
            return webSocket;
        }

        private List<String> actions() {
            return messages.stream()
                    .map(message -> message.getAsJsonObject("header").get("action").getAsString())
                    .toList();
        }

        private List<String> taskIds() {
            return messages.stream()
                    .map(message -> message.getAsJsonObject("header").get("task_id").getAsString())
                    .toList();
        }

        private final class FakeWebSocket implements WebSocket {

            private final Request request;
            private final WebSocketListener listener;

            private FakeWebSocket(Request request, WebSocketListener listener) {
                this.request = request;
                this.listener = listener;
            }

            @Override
            public Request request() {
                return request;
            }

            @Override
            public long queueSize() {
                return 0;
            }

            @Override
            public boolean send(String text) {
                JsonObject message = gson.fromJson(text, JsonObject.class);
                messages.add(message);
                String action = message.getAsJsonObject("header").get("action").getAsString();
                String taskId = message.getAsJsonObject("header").get("task_id").getAsString();
                if ("run-task".equals(action)) {
                    listener.onMessage(this, event(mismatchedTaskId ? "other-task" : taskId, "task-started"));
                } else if ("finish-task".equals(action)) {
                    listener.onMessage(this, ByteString.of(OPUS_AUDIO));
                    listener.onMessage(this, event(taskId, "task-finished"));
                }
                return true;
            }

            @Override
            public boolean send(ByteString bytes) {
                return true;
            }

            @Override
            public boolean close(int code, String reason) {
                closeCount.incrementAndGet();
                listener.onClosed(this, code, reason == null ? "" : reason);
                return true;
            }

            @Override
            public void cancel() {
            }

            private String event(String taskId, String event) {
                JsonObject header = new JsonObject();
                header.addProperty("task_id", taskId);
                header.addProperty("event", event);
                JsonObject response = new JsonObject();
                response.add("header", header);
                response.add("payload", new JsonObject());
                return gson.toJson(response);
            }
        }
    }
}
