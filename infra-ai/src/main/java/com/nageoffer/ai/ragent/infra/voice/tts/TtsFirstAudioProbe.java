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

import com.nageoffer.ai.ragent.framework.trace.RagTraceNode;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * 等待首个有效音频包，提供 TTFA 埋点边界
 */
@Component
public class TtsFirstAudioProbe {

    @RagTraceNode(name = "tts-first-audio", type = "TTS_TTFA")
    public BinaryProbeStreamBridge.ProbeResult awaitFirstAudio(BinaryProbeStreamBridge bridge,
                                                               long timeout,
                                                               TimeUnit unit) throws InterruptedException {
        return bridge.awaitFirstAudio(timeout, unit);
    }
}
