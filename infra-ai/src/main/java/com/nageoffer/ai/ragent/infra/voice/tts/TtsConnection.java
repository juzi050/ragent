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

/**
 * TTS 供应商连接的统一抽象。
 *
 * <p>连接的创建、复用和销毁由 {@link AbstractTtsClient} 管理，具体供应商
 * 只需要把自己的 SDK 适配到这个接口。</p>
 */
public interface TtsConnection<P> {

    /**
     * 使用当前连接启动一次流式合成。
     */
    void stream(P request, String text, TtsStreamListener listener) throws Exception;

    /**
     * 取消当前连接上的流式任务。
     */
    void cancel();

    /**
     * 关闭连接底层资源。对象池销毁对象时调用。
     */
    default void close() {
        cancel();
    }
}
