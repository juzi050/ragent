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

import com.nageoffer.ai.ragent.framework.errorcode.BaseErrorCode;
import com.nageoffer.ai.ragent.framework.exception.RemoteException;
import com.nageoffer.ai.ragent.infra.http.ModelClientErrorType;
import com.nageoffer.ai.ragent.infra.http.ModelClientException;
import com.nageoffer.ai.ragent.infra.model.ModelTarget;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.time.Duration;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Voice WebSocket 执行器，按 modelId 管理连接池
 */
public final class WsExecutor<C extends VoiceConnection<?, ?, ?>> implements AutoCloseable {

    private final VoiceConnectionFactory<C> connectionFactory;
    private final WsExecutorConfig config;
    private final Map<String, GenericObjectPool<C>> poolsByModelId = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public WsExecutor(VoiceConnectionFactory<C> connectionFactory,
                      WsExecutorConfig config) {
        this.connectionFactory = connectionFactory;
        this.config = config;
    }

    /**
     * 从 modelId 对应连接池借用一条空闲 WebSocket
     */
    public WsConnectionLease<C> acquire(ModelTarget target) {
        if (closed.get()) {
            throw new IllegalStateException("Voice 连接池已关闭");
        }
        String modelId = resolvePoolKey(target);

        GenericObjectPool<C> pool = poolsByModelId.computeIfAbsent(modelId, ignored -> createPool(target));
        try {
            C connection = pool.borrowObject();
            return new WsConnectionLease<>(pool, connection);
        } catch (NoSuchElementException exception) {
            Throwable cause = exception.getCause();
            // 池满（无 cause）是容量信号；create 失败（有 cause）是建连故障，透传走正常故障分类
            if (cause == null) {
                throw new VoicePoolExhaustedException("Voice 连接池无可用连接，modelId=" + modelId, exception);
            }
            if (cause instanceof ModelClientException modelClientException) {
                throw modelClientException;
            }
            throw new ModelClientException("Voice 连接创建失败，modelId=" + modelId,
                    ModelClientErrorType.PROVIDER_ERROR, null, cause);
        } catch (Exception exception) {
            if (exception instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new RemoteException("Voice 连接借用失败，modelId=" + modelId,
                    exception, BaseErrorCode.REMOTE_ERROR);
        }
    }

    public int getIdleCount(String modelId) {
        return poolsByModelId.get(modelId).getNumIdle();
    }

    public int getActiveCount(String modelId) {
        return poolsByModelId.get(modelId).getNumActive();
    }

    private String resolvePoolKey(ModelTarget target) {
        return target.id();
    }

    private GenericObjectPool<C> createPool(ModelTarget target) {
        String modelId = target.id();
        GenericObjectPoolConfig<C> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(config.maxTotalPerModel());
        poolConfig.setMaxIdle(config.maxIdlePerModel());
        poolConfig.setBlockWhenExhausted(false);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        // 移除空闲超时的连接
        if (config.idleTimeoutMs() > 0) {
            poolConfig.setMinEvictableIdleTime(Duration.ofMillis(config.idleTimeoutMs()));
            poolConfig.setTimeBetweenEvictionRuns(Duration.ofMillis(config.evictionIntervalMs()));
            poolConfig.setNumTestsPerEvictionRun(1);
        }

        return new GenericObjectPool<>(new BasePooledObjectFactory<>() {
            @Override
            public C create() throws Exception {
                C connection = null;
                try {
                    connection = connectionFactory.create(target);
                    if (!modelId.equals(connection.modelId())) {
                        throw new IllegalStateException("VoiceConnection 的 modelId 与连接池不一致，pool="
                                + modelId + "，connection=" + connection.modelId());
                    }
                    connection.connect();
                    return connection;
                } catch (Exception exception) {
                    if (connection != null) {
                        closeQuietly(connection);
                    }
                    throw exception;
                }
            }

            @Override
            public PooledObject<C> wrap(C connection) {
                return new DefaultPooledObject<>(connection);
            }

            @Override
            public boolean validateObject(PooledObject<C> pooledObject) {
                C connection = pooledObject.getObject();
                return modelId.equals(connection.modelId()) && connection.isReusable();
            }

            @Override
            public void destroyObject(PooledObject<C> pooledObject) {
                closeQuietly(pooledObject.getObject());
            }
        }, poolConfig);
    }

    private void closeQuietly(C connection) {
        try {
            connection.close();
        } catch (RuntimeException ignored) {
            // 销毁路径忽略关闭异常，保证连接池收敛
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        poolsByModelId.values().forEach(GenericObjectPool::close);
        poolsByModelId.clear();
    }
}
