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

package com.nageoffer.ai.ragent.voice.controller;

import com.nageoffer.ai.ragent.framework.context.UserContext;
import com.nageoffer.ai.ragent.infra.voice.tts.TtsTask;
import com.nageoffer.ai.ragent.infra.voice.tts.TtsStreamListener;
import com.nageoffer.ai.ragent.voice.tts.MessageSpeechService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 消息 TTS 音频流接口。
 *
 * <p>响应使用供应商原生 Ogg Opus 音频分片。浏览器端通过 Fetch 读取响应体，
 * 解码为 Float32 后交给 AudioWorklet 播放；后续 WebRTC 可复用同一 Opus 编码流。</p>
 */
@Slf4j
@RestController
@RequiredArgsConstructor
public class VoiceTtsController {

    private static final Duration STREAM_WAIT_TIMEOUT = Duration.ofMinutes(5);

    private final MessageSpeechService messageSpeechService;

    @GetMapping(value = "/voice/tts/stream", produces = "audio/ogg")
    public ResponseEntity<StreamingResponseBody> stream(@RequestParam String messageId) {
        String userId = UserContext.getUserId();
        StreamingResponseBody body = outputStream -> streamAudio(userId, messageId, outputStream);
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("audio/ogg; codecs=opus"))
                .body(body);
    }

    private void streamAudio(String userId, String messageId, OutputStream outputStream) throws IOException {
        CountDownLatch completed = new CountDownLatch(1);
        AtomicBoolean terminated = new AtomicBoolean(false);
        AtomicBoolean clientDisconnected = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<TtsTask> taskReference = new AtomicReference<>();

        TtsStreamListener listener = new TtsStreamListener() {
            @Override
            public void onAudio(byte[] audio) {
                if (terminated.get()) {
                    return;
                }
                try {
                    synchronized (outputStream) {
                        outputStream.write(audio);
                        outputStream.flush();
                    }
                } catch (IOException exception) {
                    if (terminated.compareAndSet(false, true)) {
                        clientDisconnected.set(true);
                        log.info(
                                "客户端已停止或断开 TTS 音频流，结束当前播放, userId: {}, messageId: {}, cause: {}",
                                userId,
                                messageId,
                                exception.getMessage()
                        );
                        completed.countDown();
                    }
                    cancelTask(taskReference);
                }
            }

            @Override
            public void onComplete() {
                if (terminated.compareAndSet(false, true)) {
                    completed.countDown();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                if (terminated.compareAndSet(false, true)) {
                    failure.set(throwable);
                    completed.countDown();
                }
            }
        };

        taskReference.set(messageSpeechService.stream(userId, messageId, listener));
        if (terminated.get()) {
            cancelTask(taskReference);
        }

        try {
            if (!completed.await(STREAM_WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                cancelTask(taskReference);
                throw new IOException("TTS 音频流超时");
            }
            if (clientDisconnected.get()) {
                return;
            }
            Throwable throwable = failure.get();
            if (throwable != null) {
                throw new IOException("TTS 音频流失败", throwable);
            }
        } catch (InterruptedException exception) {
            cancelTask(taskReference);
            Thread.currentThread().interrupt();
            throw new IOException("TTS 音频流被中断", exception);
        } finally {
            terminated.set(true);
        }
    }

    private void cancelTask(AtomicReference<TtsTask> taskReference) {
        TtsTask task = taskReference.get();
        if (task != null) {
            try {
                task.cancel();
            } catch (RuntimeException exception) {
                log.warn("取消 TTS 音频流失败", exception);
            }
        }
    }
}
