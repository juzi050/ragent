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

import com.alibaba.dashscope.audio.tts.SpeechSynthesisResult;
import com.alibaba.dashscope.audio.ttsv2.SpeechSynthesisParam;
import com.alibaba.dashscope.audio.ttsv2.SpeechSynthesizer;
import com.alibaba.dashscope.common.ResultCallback;
import com.alibaba.dashscope.protocol.ConnectionOptions;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;

/**
 * DashScope 全双工 TTS 连接适配器。
 *
 * <p>百炼的不同 TTS 模型共用同一种连接协议；连接池本身不在百炼客户端中，
 * 而由抽象 TTS 客户端统一管理。</p>
 */
@Component
public class DashScopeTtsConnectionFactory implements TtsConnectionFactory<SpeechSynthesisParam> {

    @Override
    public TtsConnection<SpeechSynthesisParam> create(String apiKey) {
        SpeechSynthesizer synthesizer = new SpeechSynthesizer(
                apiKey,
                ConnectionOptions.builder().build()
        );
        return new DashScopeTtsConnection(synthesizer);
    }

    private static final class DashScopeTtsConnection implements TtsConnection<SpeechSynthesisParam> {

        private final SpeechSynthesizer synthesizer;

        private DashScopeTtsConnection(SpeechSynthesizer synthesizer) {
            this.synthesizer = synthesizer;
        }

        @Override
        public void stream(SpeechSynthesisParam request, String text, TtsStreamListener listener) {
            synthesizer.updateParamAndCallback(request, new ResultCallback<>() {
                @Override
                public void onEvent(SpeechSynthesisResult result) {
                    ByteBuffer audioFrame = result.getAudioFrame();
                    if (audioFrame == null || !audioFrame.hasRemaining()) {
                        return;
                    }
                    ByteBuffer copy = audioFrame.asReadOnlyBuffer();
                    byte[] audio = new byte[copy.remaining()];
                    copy.get(audio);
                    listener.onAudio(audio);
                }

                @Override
                public void onComplete() {
                    listener.onComplete();
                }

                @Override
                public void onError(Exception exception) {
                    listener.onError(exception);
                }
            });
            synthesizer.streamingCall(text);
        }

        @Override
        public void cancel() {
            synthesizer.streamingCancel();
        }

        @Override
        public void close() {
            try {
                synthesizer.streamingCancel();
            } finally {
                if (synthesizer.getDuplexApi() != null) {
                    synthesizer.getDuplexApi().cancel();
                }
            }
        }
    }
}
