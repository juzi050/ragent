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

package com.nageoffer.ai.ragent.rag.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * 消息语音播放合成线程池配置
 */
@Data
@Configuration
public class VoicePlaybackExecutorProperties {

    /**
     * 核心线程数
     */
    @Value("${rag.executor.voice-playback.core-pool-size:2}")
    private Integer corePoolSize;

    /**
     * 最大线程数
     */
    @Value("${rag.executor.voice-playback.max-pool-size:8}")
    private Integer maxPoolSize;

    /**
     * 非核心线程空闲存活时间（秒）
     */
    @Value("${rag.executor.voice-playback.keep-alive-seconds:60}")
    private Integer keepAliveSeconds;
}
