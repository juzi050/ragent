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

import com.nageoffer.ai.ragent.framework.errorcode.BaseErrorCode;
import com.nageoffer.ai.ragent.framework.exception.RemoteException;
import com.nageoffer.ai.ragent.infra.enums.ModelCapability;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * 模型路由执行器
 * 负责在多个模型候选者之间进行调度执行，并提供故障转移（Fallback）和健康检查机制
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ModelRoutingExecutor {

    private final ModelHealthStore healthStore;
    private final WsModelExecutorRegistry wsExecutorRegistry;

    public <C, T> T executeWithFallback(
            ModelCapability capability,
            List<ModelTarget> targets,
            Function<ModelTarget, C> clientResolver,
            ModelCaller<C, T> caller) {
        String label = capability.getDisplayName();
        if (targets == null || targets.isEmpty()) {
            throw new RemoteException("No " + label + " model candidates available");
        }

        Throwable last = null;
        for (ModelTarget target : targets) {
            C client = clientResolver.apply(target);
            if (client == null) {
                log.warn("{} provider client missing: provider={}, modelId={}", label, target.candidate().getProvider(), target.id());
                continue;
            }
            if (!healthStore.allowCall(target.id())) {
                continue;
            }

            try {
                T response = caller.call(client, target);
                healthStore.markSuccess(target.id());
                return response;
            } catch (Exception e) {
                last = e;
                healthStore.markFailure(target.id());
                log.warn("{} model failed, fallback to next. modelId={}, provider={}", label, target.id(), target.candidate().getProvider(), e);
            }
        }

        throw new RemoteException(
                "All " + label + " model candidates failed: " + (last == null ? "unknown" : last.getMessage()),
                last,
                BaseErrorCode.REMOTE_ERROR
        );
    }

    /**
     * WS 长连接专用路由入口。
     *
     * <p>原有 {@link #executeWithFallback(ModelCapability, List, Function, ModelCaller)}
     * 保持不变。该方法只增加模型级 WS 线程池，并在首包前串行执行 fallback。</p>
     */
    public <C, E> WsRoutingTask<E> executeWsWithFallback(
            ModelCapability capability,
            List<ModelTarget> targets,
            Function<ModelTarget, C> clientResolver,
            WsModelCaller<C, E> caller,
            WsFirstPacketPolicy<E> firstPacketPolicy,
            WsAttemptSink<E> observer,
            WsRoutingOptions options) {
        String label = capability.getDisplayName();
        if (targets == null || targets.isEmpty()) {
            throw new RemoteException("No " + label + " model candidates available");
        }
        Objects.requireNonNull(clientResolver, "clientResolver");
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(firstPacketPolicy, "firstPacketPolicy");
        Objects.requireNonNull(observer, "observer");

        WsSession<C, E> session = new WsSession<>(
                capability,
                targets,
                clientResolver,
                caller,
                firstPacketPolicy,
                observer,
                options == null ? WsRoutingOptions.defaults() : options,
                label
        );
        session.start();
        return session;
    }

    private final class WsSession<C, E> implements WsRoutingTask<E> {

        private final ModelCapability capability;
        private final List<ModelTarget> targets;
        private final Function<ModelTarget, C> clientResolver;
        private final WsModelCaller<C, E> caller;
        private final WsFirstPacketPolicy<E> firstPacketPolicy;
        private final WsAttemptSink<E> observer;
        private final WsRoutingOptions options;
        private final String label;
        private final Object routeLock = new Object();
        private final AtomicInteger nextIndex = new AtomicInteger();
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final AtomicBoolean committed = new AtomicBoolean();
        private final AtomicBoolean terminal = new AtomicBoolean();
        private final AtomicReference<WsAttempt> current = new AtomicReference<>();
        private final CompletableFuture<Void> ready = new CompletableFuture<>();
        private final CompletableFuture<Void> completion = new CompletableFuture<>();
        private final AtomicReference<Throwable> lastError = new AtomicReference<>();
        private final long deadlineNanos;
        private volatile ScheduledFuture<?> timeoutFuture;

        private WsSession(ModelCapability capability,
                          List<ModelTarget> targets,
                          Function<ModelTarget, C> clientResolver,
                          WsModelCaller<C, E> caller,
                          WsFirstPacketPolicy<E> firstPacketPolicy,
                          WsAttemptSink<E> observer,
                          WsRoutingOptions options,
                          String label) {
            this.capability = capability;
            this.targets = List.copyOf(targets);
            this.clientResolver = clientResolver;
            this.caller = caller;
            this.firstPacketPolicy = firstPacketPolicy;
            this.observer = observer;
            this.options = options;
            this.label = label;
            this.deadlineNanos = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(options.routeTimeoutMs());
        }

        private void start() {
            timeoutFuture = wsExecutorRegistry.schedule(this::routeTimeout, options.routeTimeoutMs());
            tryNext();
        }

        @Override
        public CompletionStage<Void> ready() {
            return ready;
        }

        @Override
        public CompletionStage<Void> completion() {
            return completion;
        }

        @Override
        public void cancel() {
            if (!cancelled.compareAndSet(false, true)) {
                return;
            }
            WsAttempt attempt = current.get();
            if (attempt != null) {
                attempt.cancel();
            }
            completeCancelled();
        }

        private void tryNext() {
            synchronized (routeLock) {
                if (cancelled.get() || committed.get() || terminal.get()) {
                    return;
                }
                if (remainingMillis() <= 0) {
                    failAll(new RemoteException(label + " WS 首包探测超时", BaseErrorCode.REMOTE_ERROR));
                    return;
                }

                while (nextIndex.get() < targets.size()) {
                    ModelTarget target = targets.get(nextIndex.getAndIncrement());
                    C client;
                    try {
                        client = clientResolver.apply(target);
                    } catch (RuntimeException exception) {
                        lastError.set(exception);
                        log.warn("{} WS 客户端解析失败，切换下一个模型。modelId={}", label, target.id(), exception);
                        continue;
                    }
                    if (client == null) {
                        log.warn("{} WS provider client missing: provider={}, modelId={}",
                                label, target.candidate().getProvider(), target.id());
                        continue;
                    }

                    WsAttempt attempt = new WsAttempt(target);
                    current.set(attempt);
                    WsModelExecutorRegistry.Submission submission = wsExecutorRegistry.submit(
                            capability,
                            target,
                            () -> runAttempt(attempt, client)
                    );
                    if (!submission.accepted()) {
                        current.compareAndSet(attempt, null);
                        lastError.set(new WsCapacityException(label + " WS 线程池繁忙"));
                        continue;
                    }
                    attempt.bindSubmission(submission.future());
                    attempt.probeTimeout = wsExecutorRegistry.schedule(
                            attempt::timeout,
                            attemptTimeout(target)
                    );
                    attempt.probe.whenComplete((result, ignored) -> onProbe(attempt, result));
                    return;
                }

                failAll(lastError.get());
            }
        }

        private void runAttempt(WsAttempt attempt, C client) {
            if (!attempt.workerState.compareAndSet(WsAttempt.WORKER_QUEUED, WsAttempt.WORKER_STARTED)) {
                return;
            }
            if (cancelled.get()) {
                attempt.cancelledResult();
                attempt.finishStart();
                return;
            }

            if (!healthStore.allowCall(attempt.target.id())) {
                attempt.circuitOpen();
                attempt.finishStart();
                return;
            }
            attempt.healthPermit.set(true);
            if (attempt.cancelRequested.get() || cancelled.get()) {
                attempt.cancelledResult();
                attempt.finishStart();
                return;
            }

            WsAttemptHandle handle = null;
            try {
                handle = caller.start(client, attempt.target, attempt);
                attempt.bind(handle);
                handle.terminal().whenComplete((ignored, throwable) -> {
                    if (throwable == null) {
                        attempt.onComplete();
                    } else {
                        attempt.onError(unwrap(throwable));
                    }
                });
                handle.terminal().toCompletableFuture().get();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                if (!cancelled.get()) {
                    attempt.onError(exception);
                } else {
                    attempt.cancelledResult();
                }
            } catch (WsCapacityException exception) {
                attempt.capacity(exception);
            } catch (Exception exception) {
                attempt.onError(exception);
            } finally {
                // 供应商可能在 start() 返回前同步回调首包/错误，必须等句柄绑定后再触发路由切换。
                attempt.finishStart();
            }
        }

        private void onProbe(WsAttempt attempt, ProbeResult result) {
            if (result == null || committed.get() || terminal.get()) {
                return;
            }
            if (!attempt.startFinished.get()) {
                attempt.pendingProbe.compareAndSet(null, result);
                return;
            }
            processProbe(attempt, result);
        }

        private void processProbe(WsAttempt attempt, ProbeResult result) {
            if (result == null || committed.get() || terminal.get()) {
                return;
            }
            if (attempt.probeTimeout != null) {
                attempt.probeTimeout.cancel(false);
            }
            if (result.status == ProbeStatus.SUCCESS) {
                return;
            }
            lastError.set(result.error);
            if (cancelled.get()) {
                completeCancelled();
                return;
            }

            attempt.cancel();
            attempt.terminal.whenComplete((ignored, throwable) -> {
                synchronized (routeLock) {
                    current.compareAndSet(attempt, null);
                }
                tryNext();
            });
        }

        private boolean commit(WsAttempt attempt) {
            if (cancelled.get() || terminal.get() || current.get() != attempt) {
                return false;
            }
            if (!committed.compareAndSet(false, true)) {
                return false;
            }
            ScheduledFuture<?> timeout = timeoutFuture;
            if (timeout != null) {
                timeout.cancel(false);
            }
            ready.complete(null);
            return true;
        }

        private void completeCommitted(Throwable failure) {
            if (!terminal.compareAndSet(false, true)) {
                return;
            }
            if (failure == null) {
                completion.complete(null);
            } else {
                completion.completeExceptionally(failure);
            }
        }

        private void completeCancelled() {
            if (!terminal.compareAndSet(false, true)) {
                return;
            }
            CancellationException exception = new CancellationException(label + " WS 任务已取消");
            ready.completeExceptionally(exception);
            completion.completeExceptionally(exception);
        }

        private void dispatchEvent(WsAttempt attempt, E event) {
            try {
                observer.onEvent(event);
            } catch (RuntimeException exception) {
                handleObserverFailure(attempt, exception);
            }
        }

        private void dispatchComplete(WsAttempt attempt) {
            try {
                observer.onComplete();
            } catch (RuntimeException exception) {
                handleObserverFailure(attempt, exception);
            }
        }

        private void dispatchError(Throwable failure) {
            try {
                observer.onError(failure);
            } catch (RuntimeException exception) {
                log.warn("{} WS 下游错误回调失败", label, exception);
            }
        }

        private void handleObserverFailure(WsAttempt attempt, RuntimeException exception) {
            if (terminal.get()) {
                return;
            }
            log.warn("{} WS 下游回调失败，结束当前流", label, exception);
            completeCommitted(exception);
            cancelled.set(true);
            attempt.cancel();
        }

        private void failAll(Throwable error) {
            if (!terminal.compareAndSet(false, true)) {
                return;
            }
            Throwable finalError = error == null
                    ? new RemoteException("All " + label + " WS model candidates failed", BaseErrorCode.REMOTE_ERROR)
                    : error;
            RemoteException remote = finalError instanceof RemoteException
                    ? (RemoteException) finalError
                    : new RemoteException("All " + label + " WS model candidates failed", finalError,
                    BaseErrorCode.REMOTE_ERROR);
            ready.completeExceptionally(remote);
            completion.completeExceptionally(remote);
            if (!cancelled.get()) {
                dispatchError(remote);
            }
        }

        private void routeTimeout() {
            if (committed.get() || terminal.get() || cancelled.get()) {
                return;
            }
            lastError.set(new RemoteException(label + " WS 首包探测超时", BaseErrorCode.REMOTE_ERROR));
            WsAttempt attempt = current.get();
            failAll(lastError.get());
            cancelled.set(true);
            if (attempt != null) {
                attempt.cancel();
            }
        }

        private long remainingMillis() {
            return Math.max(0L, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));
        }

        private long attemptTimeout(ModelTarget target) {
            long configured = target.timeoutMs() == null
                    ? options.defaultFirstPacketTimeoutMs()
                    : target.timeoutMs();
            return Math.max(1L, Math.min(configured, remainingMillis()));
        }

        private Throwable unwrap(Throwable throwable) {
            if (throwable instanceof CompletionException completionException && completionException.getCause() != null) {
                return completionException.getCause();
            }
            return throwable;
        }

        private final class WsAttempt implements WsAttemptSink<E> {

            private static final int MAX_PRE_COMMIT_BUFFERED_EVENTS = 256;
            private static final int WORKER_QUEUED = 0;
            private static final int WORKER_STARTED = 1;
            private static final int WORKER_CANCELLED_BEFORE_START = 2;

            private final WsSession<C, E> session;
            private final ModelTarget target;
            private final Object lock = new Object();
            private final List<E> buffered = new ArrayList<>();
            private final CompletableFuture<ProbeResult> probe = new CompletableFuture<>();
            private final CompletableFuture<Void> terminal = new CompletableFuture<>();
            private final AtomicBoolean committed = new AtomicBoolean();
            private final AtomicBoolean terminalSignalled = new AtomicBoolean();
            private final AtomicBoolean failureRecorded = new AtomicBoolean();
            private final AtomicBoolean cancelRequested = new AtomicBoolean();
            private final AtomicBoolean startFinished = new AtomicBoolean();
            private final AtomicBoolean healthPermit = new AtomicBoolean();
            private final AtomicBoolean preCommitTerminated = new AtomicBoolean();
            private final AtomicInteger workerState = new AtomicInteger(WORKER_QUEUED);
            private final AtomicReference<WsAttemptHandle> handle = new AtomicReference<>();
            private final AtomicReference<ProbeResult> pendingProbe = new AtomicReference<>();
            private volatile Future<?> submission;
            private volatile ScheduledFuture<?> probeTimeout;

            private WsAttempt(ModelTarget target) {
                this.session = WsSession.this;
                this.target = target;
            }

            @Override
            public void onEvent(E event) {
                if (event == null || terminalSignalled.get() || preCommitTerminated.get()
                        || session.cancelled.get() || session.terminal.get() || session.current.get() != this) {
                    return;
                }
                if (committed.get()) {
                    dispatchEvent(this, event);
                    return;
                }
                boolean first;
                synchronized (lock) {
                    if (committed.get() || terminalSignalled.get() || preCommitTerminated.get()
                            || session.cancelled.get() || session.terminal.get() || session.current.get() != this) {
                        return;
                    }
                    if (buffered.size() >= MAX_PRE_COMMIT_BUFFERED_EVENTS) {
                        onError(new RemoteException(label + " WS 首包缓冲区已满",
                                BaseErrorCode.REMOTE_ERROR));
                        return;
                    }
                    buffered.add(event);
                    try {
                        first = firstPacketPolicy.isFirstPacket(event);
                    } catch (RuntimeException exception) {
                        onError(exception);
                        return;
                    }
                }
                if (first) {
                    commitAttempt();
                }
            }

            @Override
            public void onComplete() {
                boolean committedAttempt;
                synchronized (lock) {
                    if (!terminalSignalled.compareAndSet(false, true)) {
                        return;
                    }
                    committedAttempt = committed.get();
                    if (!committedAttempt) {
                        preCommitTerminated.set(true);
                    }
                }
                if (committedAttempt) {
                    if (!session.terminal.get()) {
                        dispatchComplete(this);
                    }
                    session.completeCommitted(null);
                    terminal.complete(null);
                    return;
                }
                recordFailure(new RemoteException(label + " WS 未返回有效首包", BaseErrorCode.REMOTE_ERROR));
                probe.complete(new ProbeResult(ProbeStatus.PRE_COMMIT_FAILURE, lastError.get()));
                terminal.complete(null);
            }

            @Override
            public void onError(Throwable throwable) {
                Throwable failure = throwable == null
                        ? new RemoteException(label + " WS 调用失败", BaseErrorCode.REMOTE_ERROR)
                        : throwable;
                boolean committedAttempt;
                synchronized (lock) {
                    if (!terminalSignalled.compareAndSet(false, true)) {
                        return;
                    }
                    committedAttempt = committed.get();
                    if (!committedAttempt) {
                        preCommitTerminated.set(true);
                    }
                }
                if (committedAttempt) {
                    if (!session.terminal.get()) {
                        dispatchError(failure);
                    }
                    session.completeCommitted(failure);
                    terminal.completeExceptionally(failure);
                    return;
                }
                recordFailure(failure);
                probe.complete(new ProbeResult(ProbeStatus.PRE_COMMIT_FAILURE, failure));
                terminal.completeExceptionally(failure);
            }

            private void commitAttempt() {
                List<E> toDispatch;
                synchronized (lock) {
                    if (committed.get() || terminalSignalled.get() || preCommitTerminated.get()
                            || session.cancelled.get()) {
                        return;
                    }
                    if (!session.commit(this)) {
                        return;
                    }
                    committed.set(true);
                    toDispatch = new ArrayList<>(buffered);
                    buffered.clear();
                }
                healthStore.markSuccess(target.id());
                healthPermit.set(false);
                if (probeTimeout != null) {
                    probeTimeout.cancel(false);
                }
                for (E event : toDispatch) {
                    if (session.terminal.get()) {
                        break;
                    }
                    dispatchEvent(this, event);
                }
                probe.complete(new ProbeResult(ProbeStatus.SUCCESS, null));
            }

            private void timeout() {
                synchronized (lock) {
                    if (committed.get() || terminalSignalled.get() || preCommitTerminated.get()
                            || session.cancelled.get()) {
                        return;
                    }
                    preCommitTerminated.set(true);
                }
                RemoteException timeout = new RemoteException(
                        label + " WS 首包超时，modelId=" + target.id(),
                        BaseErrorCode.REMOTE_ERROR
                );
                recordFailure(timeout);
                probe.complete(new ProbeResult(ProbeStatus.PRE_COMMIT_FAILURE, timeout));
                cancel();
            }

            private void bind(WsAttemptHandle attemptHandle) {
                handle.set(attemptHandle);
                if (cancelRequested.get()) {
                    attemptHandle.cancel();
                }
            }

            private void bindSubmission(Future<?> future) {
                submission = future;
                if (workerState.get() == WORKER_CANCELLED_BEFORE_START) {
                    future.cancel(false);
                }
            }

            private void finishStart() {
                if (!startFinished.compareAndSet(false, true)) {
                    return;
                }
                ProbeResult pending = pendingProbe.getAndSet(null);
                if (pending != null) {
                    processProbe(this, pending);
                }
            }

            private void cancel() {
                cancelRequested.set(true);
                releaseHealthPermit();
                WsAttemptHandle attemptHandle = handle.get();
                if (attemptHandle != null) {
                    attemptHandle.cancel();
                    return;
                }
                if (workerState.compareAndSet(WORKER_QUEUED, WORKER_CANCELLED_BEFORE_START)) {
                    Future<?> submitted = submission;
                    if (submitted != null) {
                        submitted.cancel(false);
                    }
                    if (!probe.isDone()) {
                        probe.complete(new ProbeResult(ProbeStatus.CANCELLED, null));
                    }
                    if (terminalSignalled.compareAndSet(false, true)) {
                        terminal.complete(null);
                    }
                    finishStart();
                }
            }

            private void circuitOpen() {
                probe.complete(new ProbeResult(ProbeStatus.CIRCUIT_OPEN, null));
                terminal.complete(null);
            }

            private void capacity(Throwable failure) {
                releaseHealthPermit();
                lastError.set(failure);
                probe.complete(new ProbeResult(ProbeStatus.CAPACITY, failure));
                terminal.complete(null);
            }

            private void cancelledResult() {
                releaseHealthPermit();
                probe.complete(new ProbeResult(ProbeStatus.CANCELLED, null));
                terminal.complete(null);
            }

            private void releaseHealthPermit() {
                if (healthPermit.compareAndSet(true, false)) {
                    healthStore.releaseCall(target.id());
                }
            }

            private void recordFailure(Throwable failure) {
                lastError.set(failure);
                if (!session.cancelled.get() && failureRecorded.compareAndSet(false, true)) {
                    healthPermit.set(false);
                    healthStore.markFailure(target.id());
                } else if (session.cancelled.get()) {
                    releaseHealthPermit();
                }
            }

        }

        private enum ProbeStatus {
            SUCCESS,
            PRE_COMMIT_FAILURE,
            CIRCUIT_OPEN,
            CAPACITY,
            CANCELLED
        }

        private record ProbeResult(ProbeStatus status, Throwable error) {
        }
    }
}
