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

import com.nageoffer.ai.ragent.infra.config.AIModelProperties;
import com.nageoffer.ai.ragent.infra.enums.ModelProvider;
import com.nageoffer.ai.ragent.infra.http.HttpResponseHelper;
import com.nageoffer.ai.ragent.infra.http.ModelClientErrorType;
import com.nageoffer.ai.ragent.infra.http.ModelClientException;
import com.nageoffer.ai.ragent.infra.model.ModelTarget;
import com.nageoffer.ai.ragent.infra.voice.WsExecutorConfig;
import jakarta.annotation.PreDestroy;
import okhttp3.OkHttpClient;
import okhttp3.WebSocket;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executor;

/**
 * 阿里云百炼 CosyVoice TTS 适配器
 */
@Component
public class BaiLianTtsClient extends AbstractWsTtsClient<BaiLianTtsTaskParam, BaiLianTtsConnection> {

    private final WebSocket.Factory webSocketFactory;
    private final AIModelProperties.WebSocketConfig websocketConfig;

    public BaiLianTtsClient(@Qualifier("streamingHttpClient") WebSocket.Factory webSocketFactory,
                            @Qualifier("wsLifecycleExecutor") Executor taskExecutor,
                            AIModelProperties properties) {
        super(taskExecutor, new WsExecutorConfig(
                properties.getWebsocket().getMaxTotalPerModel(),
                properties.getWebsocket().getMaxIdlePerModel(),
                properties.getWebsocket().getIdleTimeoutMs()));
        this.webSocketFactory = webSocketFactory;
        this.websocketConfig = properties.getWebsocket();
    }

    @Override
    public String provider() {
        return ModelProvider.BAI_LIAN.getId();
    }

    @Override
    protected BaiLianTtsConnection createConnection(ModelTarget target) {
        return new BaiLianTtsConnection(target, webSocketFactory, websocketConfig);
    }

    @Override
    protected BaiLianTtsTaskParam buildTaskParam(TtsRequest request, ModelTarget target) {
        return new BaiLianTtsTaskParam(
                HttpResponseHelper.requireModel(target, "bailian TTS"),
                requireVoice(target),
                resolveAudioFormat(target)
        );
    }

    /**
     * 音色由候选模型配置提供 缺失即失败
     */
    private String requireVoice(ModelTarget target) {
        String voice = target.candidate().getVoice();
        if (voice == null || voice.isBlank()) {
            throw new ModelClientException("bailian TTS 未配置默认音色，modelId=" + target.id(),
                    ModelClientErrorType.INVALID_RESPONSE, null);
        }
        return voice;
    }

    private String resolveAudioFormat(ModelTarget target) {
        String format = target.candidate().getAudioFormat();
        if (format == null || format.isBlank()) {
            format = "opus";
        }
        return format;
    }

    @Override
    protected byte[] convertAudio(byte[] providerAudio, ModelTarget target) {
        return providerAudio;
    }

    @PreDestroy
    public void destroy() {
        close();
    }
}
