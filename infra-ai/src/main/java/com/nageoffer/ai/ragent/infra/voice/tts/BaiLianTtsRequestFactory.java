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

import com.alibaba.dashscope.audio.ttsv2.SpeechSynthesisAudioFormat;
import com.alibaba.dashscope.audio.ttsv2.SpeechSynthesisParam;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 百炼 TTS 标准请求到 DashScope 请求参数的转换器。
 */
@Component
public class BaiLianTtsRequestFactory implements TtsProviderRequestFactory<SpeechSynthesisParam> {

    @Override
    public SpeechSynthesisParam create(TtsSynthesisRequest request) {
        return new OpusSpeechSynthesisParam(request.model(), request.voice());
    }

    /**
     * DashScope SDK 默认会附加采样率、音量、语速等可选参数，这里仅保留百炼协议必需参数和 Opus 格式。
     */
    private static final class OpusSpeechSynthesisParam extends SpeechSynthesisParam {

        private OpusSpeechSynthesisParam(String model, String voice) {
            super(SpeechSynthesisParam.builder()
                    .model(model)
                    .voice(voice)
                    .format(SpeechSynthesisAudioFormat.OGG_OPUS_24KHZ_MONO_32KBPS));
        }

        @Override
        public Map<String, Object> getParameters() {
            return Map.of(
                    "text_type", "PlainText",
                    "voice", getVoice(),
                    "format", "opus"
            );
        }
    }
}
