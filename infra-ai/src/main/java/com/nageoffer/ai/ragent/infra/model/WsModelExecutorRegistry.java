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

package com.nageoffer.ai.ragent.infra.model;

import com.nageoffer.ai.ragent.infra.enums.ModelCapability;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * WS 长连接模型执行池注册表。
 *
 * <p>每个 capability + modelId 独立限流。普通模型调用不使用该注册表。</p>
 */
@Component
public class WsModelExecutorRegistry {

    private static final int DEFAULT_THREADS = 8;
    private static final int DEFAULT_QUEUE_CAPACITY = 0;

    private final Map<String, ThreadPoolExecutor> executors = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;

    public WsModelExecutorRegistry() {
        this(DEFAULT_THREADS, DEFAULT_QUEUE_CAPACITY);
    }

    public WsModelExecutorRegistry(int defaultThreads, int defaultQueueCapacity) {
        if (defaultThreads <= 0 || defaultQueueCapacity < 0) {
            throw new IllegalArgumentException("WS 线程池参数不合法");
        }
        this.defaultThreads = defaultThreads;
        this.defaultQueueCapacity = defaultQueueCapacity;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "ws-routing-timeout");
            thread.setDaemon(true);
            return thread;
        });
    }

    private final int defaultThreads;
    private final int defaultQueueCapacity;

    public Submission submit(ModelCapability capability, ModelTarget target, Runnable task) {
        ThreadPoolExecutor executor = executors.computeIfAbsent(
                key(capability, target),
                ignored -> createExecutor(capability, target)
        );
        try {
            Future<?> future = executor.submit(task);
            return Submission.accepted(future);
        } catch (RejectedExecutionException exception) {
            return Submission.rejected();
        }
    }

    private ThreadPoolExecutor createExecutor(ModelCapability capability, ModelTarget target) {
        int threads = defaultThreads;
        int queueCapacity = defaultQueueCapacity;
        AtomicInteger threadNumber = new AtomicInteger();
        String prefix = "ws-" + capability.name().toLowerCase() + "-" + normalize(target.id()) + "-";
        return new ThreadPoolExecutor(
                threads,
                threads,
                0L,
                TimeUnit.MILLISECONDS,
                queueCapacity == 0 ? new SynchronousQueue<>() : new ArrayBlockingQueue<>(queueCapacity),
                runnable -> {
                    Thread thread = new Thread(runnable, prefix + threadNumber.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    public java.util.concurrent.ScheduledFuture<?> schedule(Runnable task, long delayMs) {
        return scheduler.schedule(task, delayMs, TimeUnit.MILLISECONDS);
    }

    private String key(ModelCapability capability, ModelTarget target) {
        return capability.name() + ":" + target.id();
    }

    private String normalize(String value) {
        return value.replaceAll("[^a-zA-Z0-9-_]", "-");
    }

    @PreDestroy
    public void destroy() {
        executors.values().forEach(ExecutorService::shutdownNow);
        executors.clear();
        scheduler.shutdownNow();
    }

    public record Submission(boolean accepted, Future<?> future) {

        static Submission accepted(Future<?> future) {
            return new Submission(true, future);
        }

        static Submission rejected() {
            return new Submission(false, null);
        }
    }
}
