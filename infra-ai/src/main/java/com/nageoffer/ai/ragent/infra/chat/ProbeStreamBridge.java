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

package com.nageoffer.ai.ragent.infra.chat;

import com.nageoffer.ai.ragent.infra.model.AbstractProbeStreamBridge;

import java.util.concurrent.TimeUnit;

/**
 * 流式首包探测桥接器
 */
public final class ProbeStreamBridge extends AbstractProbeStreamBridge implements StreamCallback {

    private final StreamCallback downstream;

    ProbeStreamBridge(StreamCallback downstream) {
        this.downstream = downstream;
    }

    @Override
    public void onContent(String content) {
        acceptPacket(() -> downstream.onContent(content));
    }

    @Override
    public void onThinking(String content) {
        acceptPacket(() -> downstream.onThinking(content));
    }

    @Override
    public void onComplete() {
        acceptComplete(downstream::onComplete);
    }

    @Override
    public void onError(Throwable t) {
        acceptError(t, () -> downstream.onError(t));
    }

    ProbeResult awaitFirstPacket(long timeout, TimeUnit unit) throws InterruptedException {
        ProbeResult result = awaitProbe(timeout, unit);
        if (result.isSuccess()) {
            commit();
        }
        return result;
    }
}
