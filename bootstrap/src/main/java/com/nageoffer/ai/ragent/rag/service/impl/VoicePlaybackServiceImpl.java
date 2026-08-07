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

package com.nageoffer.ai.ragent.rag.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.core.util.IdUtil;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.nageoffer.ai.ragent.framework.context.UserContext;
import com.nageoffer.ai.ragent.framework.exception.ClientException;
import com.nageoffer.ai.ragent.framework.web.SseEmitterSender;
import com.nageoffer.ai.ragent.infra.chat.StreamCancellationHandle;
import com.nageoffer.ai.ragent.infra.config.AIModelProperties;
import com.nageoffer.ai.ragent.infra.voice.tts.TtsCallback;
import com.nageoffer.ai.ragent.infra.voice.tts.TtsRequest;
import com.nageoffer.ai.ragent.infra.voice.tts.TtsService;
import com.nageoffer.ai.ragent.infra.voice.tts.TtsTask;
import com.nageoffer.ai.ragent.rag.dao.entity.ConversationMessageDO;
import com.nageoffer.ai.ragent.rag.dao.mapper.ConversationMessageMapper;
import com.nageoffer.ai.ragent.rag.dto.AudioFramePayload;
import com.nageoffer.ai.ragent.rag.dto.AudioMetaPayload;
import com.nageoffer.ai.ragent.rag.enums.SSEEventType;
import com.nageoffer.ai.ragent.rag.service.VoicePlaybackService;
import com.nageoffer.ai.ragent.rag.service.handler.StreamTaskManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Base64;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 消息语音播放服务默认实现
 */
@Slf4j
@Service
public class VoicePlaybackServiceImpl implements VoicePlaybackService {

    private static final String ROLE_ASSISTANT = "assistant";

    private final ConversationMessageMapper conversationMessageMapper;
    private final TtsService ttsService;
    private final StreamTaskManager taskManager;
    private final AIModelProperties aiModelProperties;
    private final Executor voicePlaybackExecutor;

    public VoicePlaybackServiceImpl(ConversationMessageMapper conversationMessageMapper,
                                    TtsService ttsService,
                                    StreamTaskManager taskManager,
                                    AIModelProperties aiModelProperties,
                                    @Qualifier("voicePlaybackExecutor") Executor voicePlaybackExecutor) {
        this.conversationMessageMapper = conversationMessageMapper;
        this.ttsService = ttsService;
        this.taskManager = taskManager;
        this.aiModelProperties = aiModelProperties;
        this.voicePlaybackExecutor = voicePlaybackExecutor;
    }

    @Override
    public void play(String messageId, SseEmitter emitter) {
        String taskId = IdUtil.getSnowflakeNextIdStr();
        String userId = UserContext.getUserId();

        ConversationMessageDO message = loadAssistantMessage(messageId, userId);
        String text = message.getContent();
        if (StrUtil.isBlank(text)) {
            throw new ClientException("消息内容为空，无法播放");
        }
        String audioFormat = resolveAudioFormat();
        log.info("播放任务发起，taskId={}，messageId={}，userId={}，textLength={}，audioFormat={}",
                taskId, messageId, userId, text.length(), audioFormat);

        SseEmitterSender sender = new SseEmitterSender(emitter);
        taskManager.register(taskId, sender, () -> null);

        // 独立线程池异步合成
        try {
            voicePlaybackExecutor.execute(() -> synthesizeAndStream(taskId, userId, text, audioFormat, sender));
        } catch (RejectedExecutionException exception) {
            log.error("播放任务线程池拒绝，taskId={}", taskId, exception);
            taskManager.unregister(taskId);
            sender.fail(exception);
        }
    }

    @Override
    public void stop(String taskId) {
        taskManager.cancel(taskId);
    }

    private void synthesizeAndStream(String taskId, String userId, String text, String audioFormat, SseEmitterSender sender) {
        if (taskManager.isCancelled(taskId)) {
            return;
        }
        AtomicBoolean metaSent = new AtomicBoolean();
        AtomicBoolean metaLogged = new AtomicBoolean();
        AtomicInteger audioFrameCount = new AtomicInteger();
        AtomicLong audioBytes = new AtomicLong();
        TtsCallback callback = buildCallback(taskId, sender, audioFormat, metaSent, metaLogged, audioFrameCount, audioBytes);

        TtsTask task;
        try {
            task = ttsService.synthesize(new TtsRequest(text), callback);
            log.info("播放任务已启动，taskId={}，textLength={}", taskId, text.length());
        } catch (RuntimeException exception) {
            log.error("播放任务启动失败，taskId={}，textLength={}", taskId, text.length(), exception);
            taskManager.unregister(taskId);
            sender.fail(exception);
            return;
        }

        // 绑定取消句柄
        taskManager.bindHandle(taskId, toCancellationHandle(taskId, task));
    }

    private TtsCallback buildCallback(String taskId, SseEmitterSender sender, String audioFormat,
                                      AtomicBoolean metaSent, AtomicBoolean metaLogged,
                                      AtomicInteger audioFrameCount, AtomicLong audioBytes) {
        return new TtsCallback() {
            @Override
            public void onAudio(byte[] opusAudio) {
                if (taskManager.isCancelled(taskId)) {
                    return;
                }
                if (opusAudio == null || opusAudio.length == 0) {
                    return;
                }
                // 首帧前下发音频元信息
                if (metaSent.compareAndSet(false, true)) {
                    sender.sendEvent(SSEEventType.AUDIO_META.value(), new AudioMetaPayload(taskId, audioFormat, null));
                }
                if (metaLogged.compareAndSet(false, true)) {
                    log.info("播放任务首帧下发，taskId={}，frameBytes={}", taskId, opusAudio.length);
                }
                audioFrameCount.incrementAndGet();
                audioBytes.addAndGet(opusAudio.length);
                sender.sendEvent(SSEEventType.AUDIO.value(), new AudioFramePayload(
                        Base64.getEncoder().encodeToString(opusAudio)));
            }

            @Override
            public void onComplete() {
                if (taskManager.isCancelled(taskId)) {
                    return;
                }
                log.info("播放任务完成，taskId={}", taskId);
                sender.sendEvent(SSEEventType.DONE.value(), "[DONE]");
                taskManager.unregister(taskId);
                sender.complete();
            }

            @Override
            public void onError(Throwable throwable) {
                if (taskManager.isCancelled(taskId)) {
                    return;
                }
                log.error("播放任务失败，taskId={}，已收帧数={}，已收字节={}",
                        taskId, audioFrameCount.get(), audioBytes.get(), throwable);
                taskManager.unregister(taskId);
                sender.fail(throwable);
            }
        };
    }

    private StreamCancellationHandle toCancellationHandle(String taskId, TtsTask task) {
        // 停止播放归还连接复用 连接异常由连接层销毁
        return () -> {
            try {
                task.cancel();
            } catch (RuntimeException exception) {
                log.warn("播放任务取消失败，taskId={}", taskId, exception);
            }
        };
    }

    /**
     * 音频格式由 tts 默认候选配置提供 缺失即失败
     */
    private String resolveAudioFormat() {
        AIModelProperties.ModelGroup tts = aiModelProperties.getTts();
        return tts.getCandidates().stream()
                .filter(candidate -> candidate.getId().equals(tts.getDefaultModel()))
                .map(AIModelProperties.ModelCandidate::getAudioFormat)
                .filter(StrUtil::isNotBlank)
                .findFirst()
                .orElseThrow(() -> new ClientException("TTS 音频格式未配置，defaultModel=" + tts.getDefaultModel()));
    }

    /**
     * 定位当前用户的 assistant 消息
     */
    private ConversationMessageDO loadAssistantMessage(String messageId, String userId) {
        ConversationMessageDO message = conversationMessageMapper.selectOne(
                Wrappers.lambdaQuery(ConversationMessageDO.class)
                        .eq(ConversationMessageDO::getId, messageId)
                        .eq(ConversationMessageDO::getUserId, userId)
                        .eq(ConversationMessageDO::getRole, ROLE_ASSISTANT)
                        .eq(ConversationMessageDO::getDeleted, 0)
        );
        if (message == null) {
            throw new ClientException("消息不存在");
        }
        return message;
    }
}
