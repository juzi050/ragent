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
import com.nageoffer.ai.ragent.infra.http.HttpResponseHelper;
import com.nageoffer.ai.ragent.infra.model.ModelTarget;
import com.nageoffer.ai.ragent.infra.model.WsAttemptHandle;
import com.nageoffer.ai.ragent.infra.model.WsCapacityException;
import jakarta.annotation.PreDestroy;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * TTS WebSocket 客户端模板，统一管理请求构建、连接池、回调和取消生命周期。
 */
public abstract class AbstractTtsClient<P> implements TtsClient {

    private static final int CONNECTION_POOL_MAX_TOTAL = 8;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final TtsProviderRequestFactory<P> requestFactory;
    private final TtsConnectionFactory<P> connectionFactory;
    private final Map<String, GenericObjectPool<TtsConnection<P>>> connectionPools = new ConcurrentHashMap<>();

    protected AbstractTtsClient(TtsProviderRequestFactory<P> requestFactory,
                                TtsConnectionFactory<P> connectionFactory) {
        this.requestFactory = requestFactory;
        this.connectionFactory = connectionFactory;
    }

    @Override
    public abstract String provider();

    @Override
    public final WsAttemptHandle startWsAttempt(String text,
                                                TtsStreamListener listener,
                                                ModelTarget target) {
        TtsSynthesisRequest synthesisRequest = buildRequest(text, target);
        P providerRequest = requestFactory.create(synthesisRequest);
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicBoolean terminalSignalled = new AtomicBoolean();
        AtomicReference<ConnectionLease<P>> leaseReference = new AtomicReference<>();
        CompletableFuture<Void> terminal = new CompletableFuture<>();

        TtsStreamListener callback = new TtsStreamListener() {
            @Override
            public void onAudio(byte[] audio) {
                if (!cancelled.get()) {
                    listener.onAudio(audio);
                }
            }

            @Override
            public void onComplete() {
                if (!terminalSignalled.compareAndSet(false, true)) {
                    return;
                }
                ConnectionLease<P> lease = leaseReference.getAndSet(null);
                if (lease != null) {
                    release(lease, !cancelled.get());
                }
                if (!cancelled.get()) {
                    listener.onComplete();
                }
                terminal.complete(null);
            }

            @Override
            public void onError(Throwable throwable) {
                if (!terminalSignalled.compareAndSet(false, true)) {
                    return;
                }
                ConnectionLease<P> lease = leaseReference.getAndSet(null);
                if (lease != null) {
                    release(lease, false);
                }
                if (!cancelled.get()) {
                    listener.onError(throwable);
                }
                terminal.completeExceptionally(throwable);
            }
        };

        try {
            GenericObjectPool<TtsConnection<P>> pool = connectionPool(target.id(), synthesisRequest.apiKey());
            ConnectionLease<P> lease;
            try {
                lease = new ConnectionLease<>(pool, pool.borrowObject());
            } catch (NoSuchElementException exception) {
                terminalSignalled.set(true);
                WsCapacityException capacityException = new WsCapacityException("TTS 连接池无可用连接", exception);
                terminal.completeExceptionally(capacityException);
                throw capacityException;
            }
            leaseReference.set(lease);
            lease.connection().stream(providerRequest, synthesisRequest.text(), callback);
        } catch (WsCapacityException exception) {
            throw exception;
        } catch (Exception exception) {
            ConnectionLease<P> lease = leaseReference.getAndSet(null);
            if (lease != null) {
                release(lease, false);
            }
            if (terminalSignalled.compareAndSet(false, true)) {
                listener.onError(exception);
                terminal.completeExceptionally(exception);
            }
            throw new IllegalStateException("TTS WS 任务启动失败", exception);
        }

        return new WsAttemptHandle() {
            @Override
            public java.util.concurrent.CompletionStage<Void> terminal() {
                return terminal;
            }

            @Override
            public void cancel() {
                if (!cancelled.compareAndSet(false, true)
                        || !terminalSignalled.compareAndSet(false, true)) {
                    return;
                }
                ConnectionLease<P> lease = leaseReference.getAndSet(null);
                if (lease != null) {
                    try {
                        lease.connection().cancel();
                    } catch (RuntimeException exception) {
                        log.info("TTS WS 任务取消期间产生异常，按已取消处理。modelId: {}, cause: {}",
                                target.id(), exception.getMessage());
                    } finally {
                        release(lease, false);
                    }
                }
                terminal.complete(null);
            }
        };
    }

    private TtsSynthesisRequest buildRequest(String text, ModelTarget target) {
        AIModelProperties.ProviderConfig providerConfig = HttpResponseHelper.requireProvider(target, provider());
        HttpResponseHelper.requireApiKey(providerConfig, provider());
        return new TtsSynthesisRequest(
                providerConfig.getApiKey(),
                HttpResponseHelper.requireModel(target, provider()),
                providerConfig.getTts().getVoice(),
                text
        );
    }

    private GenericObjectPool<TtsConnection<P>> connectionPool(String modelId, String apiKey) {
        return connectionPools.computeIfAbsent(modelId + "::" + apiKey, ignored -> createConnectionPool(apiKey));
    }

    private GenericObjectPool<TtsConnection<P>> createConnectionPool(String apiKey) {
        GenericObjectPoolConfig<TtsConnection<P>> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(CONNECTION_POOL_MAX_TOTAL);
        config.setMaxIdle(CONNECTION_POOL_MAX_TOTAL);
        config.setBlockWhenExhausted(false);

        return new GenericObjectPool<>(new BasePooledObjectFactory<>() {
            @Override
            public TtsConnection<P> create() throws Exception {
                return connectionFactory.create(apiKey);
            }

            @Override
            public PooledObject<TtsConnection<P>> wrap(TtsConnection<P> connection) {
                return new DefaultPooledObject<>(connection);
            }

            @Override
            public void destroyObject(PooledObject<TtsConnection<P>> pooledObject) {
                pooledObject.getObject().close();
            }
        }, config);
    }

    private void release(ConnectionLease<P> lease, boolean reusable) {
        try {
            if (reusable) {
                lease.pool().returnObject(lease.connection());
            } else {
                lease.pool().invalidateObject(lease.connection());
            }
        } catch (Exception exception) {
            log.warn("TTS 连接释放失败，provider: {}", provider(), exception);
        }
    }

    @PreDestroy
    public final void destroy() {
        connectionPools.values().forEach(pool -> {
            try {
                pool.close();
            } catch (Exception exception) {
                log.warn("关闭 TTS 连接池失败", exception);
            }
        });
        connectionPools.clear();
    }

    private record ConnectionLease<P>(GenericObjectPool<TtsConnection<P>> pool,
                                      TtsConnection<P> connection) {
    }
}
