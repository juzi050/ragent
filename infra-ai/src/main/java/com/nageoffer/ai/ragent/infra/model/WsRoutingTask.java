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

import java.util.concurrent.CompletionStage;

/**
 * 一次逻辑 WS 路由任务。
 *
 * @param <E> 流式输出事件类型
 */
public interface WsRoutingTask<E> {

    /**
     * 首包成功后完成；所有候选在首包前失败时异常完成。
     */
    CompletionStage<Void> ready();

    /**
     * 逻辑任务终态完成。
     */
    CompletionStage<Void> completion();

    void cancel();
}
