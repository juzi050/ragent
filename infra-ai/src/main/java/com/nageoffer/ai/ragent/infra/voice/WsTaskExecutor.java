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

package com.nageoffer.ai.ragent.infra.voice;

import com.nageoffer.ai.ragent.infra.model.ModelTarget;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;

/**
 * 在池化 WebSocket 连接上启动一次独立任务
 */
public class WsTaskExecutor<P, I, O, C extends VoiceConnection<P, I, O>> implements AutoCloseable {

    private final WsExecutor<C> wsExecutor;
    private final Executor taskExecutor;

    public WsTaskExecutor(VoiceConnectionFactory<C> connectionFactory,
                          WsExecutorConfig poolConfig,
                          Executor taskExecutor) {
        this(new WsExecutor<>(connectionFactory, poolConfig), taskExecutor);
    }

    WsTaskExecutor(WsExecutor<C> wsExecutor, Executor taskExecutor) {
        this.wsExecutor = wsExecutor;
        this.taskExecutor = taskExecutor;
    }

    public WsTaskSession<I> openTask(ModelTarget target,
                                     P taskParam,
                                     VoiceStreamCallback<O> callback) {
        return openTask(target, taskParam, callback, () -> false);
    }

    public WsTaskSession<I> openTask(ModelTarget target,
                                     P taskParam,
                                     VoiceStreamCallback<O> callback,
                                     BooleanSupplier invalidateOnFinish) {
        WsConnectionLease<C> lease = wsExecutor.acquire(target);
        String taskId = generateTaskId();
        try {
            lease.connection().startTask(taskId, taskParam, callback);
            return new WsTaskSession<>(taskId, lease.connection(), lease, taskExecutor, invalidateOnFinish);
        } catch (RuntimeException exception) {
            lease.invalidate();
            lease.close();
            throw exception;
        }
    }

    protected String generateTaskId() {
        return UUID.randomUUID().toString();
    }

    @Override
    public void close() {
        wsExecutor.close();
    }
}
