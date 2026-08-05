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

package com.nageoffer.ai.ragent.voice.tts;

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.nageoffer.ai.ragent.framework.exception.ClientException;
import com.nageoffer.ai.ragent.infra.voice.tts.TtsService;
import com.nageoffer.ai.ragent.infra.voice.tts.TtsStreamListener;
import com.nageoffer.ai.ragent.infra.voice.tts.TtsTask;
import com.nageoffer.ai.ragent.rag.dao.entity.ConversationMessageDO;
import com.nageoffer.ai.ragent.rag.dao.mapper.ConversationMessageMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/**
 * 助手消息朗读编排：读取消息并启动 TTS 音频流。
 *
 * <p>该服务不依赖 WebRTC 或前端播放方式，HTTP 流、WebRTC 等传输层都可以复用。</p>
 */
@Service
@RequiredArgsConstructor
public class MessageSpeechService {

    private static final String ROLE_ASSISTANT = "assistant";

    private final ConversationMessageMapper messageMapper;
    private final TtsService ttsService;

    public TtsTask stream(String userId, String messageId, TtsStreamListener listener) {
        ConversationMessageDO message = loadMessage(userId, messageId);
        String text = message.getContent();
        if (StrUtil.isBlank(text)) {
            throw new ClientException("消息没有可朗读的内容");
        }
        return ttsService.streamTts(text, listener);
    }

    private ConversationMessageDO loadMessage(String userId, String messageId) {
        ConversationMessageDO message = messageMapper.selectOne(
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
