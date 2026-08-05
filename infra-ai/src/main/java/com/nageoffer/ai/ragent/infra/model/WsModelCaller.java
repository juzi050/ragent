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

/**
 * 启动一次 WS Attempt 的调用器。
 *
 * <p>调用发生在 WS Runtime worker 内，不能再次把任务提交到模型线程池。</p>
 */
@FunctionalInterface
public interface WsModelCaller<C, E> {

    WsAttemptHandle start(C client,
                          ModelTarget target,
                          WsAttemptSink<E> sink) throws Exception;
}
