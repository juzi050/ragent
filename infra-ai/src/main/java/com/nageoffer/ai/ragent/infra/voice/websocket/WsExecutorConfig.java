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

package com.nageoffer.ai.ragent.infra.voice.websocket;

/**
 * 单个 modelId 的 WebSocket 连接池容量
 *
 * @param maxTotalPerModel   池最大连接数
 * @param maxIdlePerModel    池最大空闲连接数
 * @param idleTimeoutMs      空闲连接驱逐超时（毫秒） 0 表示不驱逐
 * @param evictionIntervalMs 空闲驱逐扫描间隔（毫秒）
 */
public record WsExecutorConfig(int maxTotalPerModel, int maxIdlePerModel,
                               long idleTimeoutMs, long evictionIntervalMs) {
}
