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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 在路由层确认候选前隔离其音频和终态事件
 */
public final class BinaryProbeStreamBridge implements TtsCallback {

    private enum Disposition {
        PENDING,
        COMMITTED,
        DISCARDED
    }

    private final TtsCallback downstream;
    private final CompletableFuture<ProbeResult> probe = new CompletableFuture<>();
    private final Object lock = new Object();
    private final List<Runnable> buffer = new ArrayList<>();
    private Disposition disposition = Disposition.PENDING;
    private boolean terminated;

    public BinaryProbeStreamBridge(TtsCallback downstream) {
        this.downstream = downstream;
    }

    @Override
    public void onAudio(byte[] audio) {
        if (audio == null || audio.length == 0) {
            return;
        }
        synchronized (lock) {
            if (terminated || disposition == Disposition.DISCARDED) {
                return;
            }
            probe.complete(ProbeResult.success());
            bufferOrDispatch(() -> downstream.onAudio(audio));
        }
    }

    @Override
    public void onComplete() {
        synchronized (lock) {
            if (terminated || disposition == Disposition.DISCARDED) {
                return;
            }
            terminated = true;
            probe.complete(ProbeResult.noContent());
            bufferOrDispatch(downstream::onComplete);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        synchronized (lock) {
            if (terminated || disposition == Disposition.DISCARDED) {
                return;
            }
            terminated = true;
            probe.complete(ProbeResult.error(throwable));
            bufferOrDispatch(() -> downstream.onError(throwable));
        }
    }

    public ProbeResult awaitFirstAudio(long timeout, TimeUnit unit) throws InterruptedException {
        try {
            return probe.get(timeout, unit);
        } catch (TimeoutException exception) {
            return ProbeResult.timeout();
        } catch (ExecutionException exception) {
            return ProbeResult.error(exception.getCause());
        }
    }

    public void commit() {
        synchronized (lock) {
            if (disposition != Disposition.PENDING) {
                return;
            }
            disposition = Disposition.COMMITTED;
            buffer.forEach(Runnable::run);
            buffer.clear();
        }
    }

    public void discard() {
        synchronized (lock) {
            if (disposition != Disposition.PENDING) {
                return;
            }
            disposition = Disposition.DISCARDED;
            buffer.clear();
        }
    }

    private void bufferOrDispatch(Runnable action) {
        if (disposition == Disposition.COMMITTED) {
            action.run();
        } else {
            buffer.add(action);
        }
    }

    public static final class ProbeResult {

        public enum Type {
            SUCCESS,
            ERROR,
            TIMEOUT,
            NO_CONTENT
        }

        private final Type type;
        private final Throwable error;

        private ProbeResult(Type type, Throwable error) {
            this.type = type;
            this.error = error;
        }

        public Type type() {
            return type;
        }

        public Throwable error() {
            return error;
        }

        public boolean isSuccess() {
            return type == Type.SUCCESS;
        }

        private static ProbeResult success() {
            return new ProbeResult(Type.SUCCESS, null);
        }

        private static ProbeResult error(Throwable throwable) {
            return new ProbeResult(Type.ERROR, throwable);
        }

        private static ProbeResult timeout() {
            return new ProbeResult(Type.TIMEOUT, null);
        }

        private static ProbeResult noContent() {
            return new ProbeResult(Type.NO_CONTENT, null);
        }
    }
}
