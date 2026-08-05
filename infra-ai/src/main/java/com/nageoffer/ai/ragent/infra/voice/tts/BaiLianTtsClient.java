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

import com.alibaba.dashscope.audio.ttsv2.SpeechSynthesisParam;
import com.nageoffer.ai.ragent.infra.enums.ModelProvider;
import org.springframework.stereotype.Service;

/**
 * 百炼 TTS 参数适配客户端。
 *
 * <p>线程池、连接池和连接生命周期均由抽象 TTS 客户端管理。</p>
 */
@Service
public class BaiLianTtsClient extends AbstractTtsClient<SpeechSynthesisParam> {

    public BaiLianTtsClient(BaiLianTtsRequestFactory requestFactory,
                            DashScopeTtsConnectionFactory connectionFactory) {
        super(requestFactory, connectionFactory);
    }

    @Override
    public String provider() {
        return ModelProvider.BAI_LIAN.getId();
    }
}
