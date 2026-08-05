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
 * WS 路由参数。
 *
 * @param defaultFirstPacketTimeoutMs 未配置候选超时时使用的首包超时
 * @param routeTimeoutMs              首包提交前的逻辑任务总预算
 */
public record WsRoutingOptions(long defaultFirstPacketTimeoutMs,
                               long routeTimeoutMs) {

    public WsRoutingOptions {
        if (defaultFirstPacketTimeoutMs <= 0 || routeTimeoutMs <= 0) {
            throw new IllegalArgumentException("WS 路由超时必须大于 0");
        }
    }

    public static WsRoutingOptions defaults() {
        return new WsRoutingOptions(10_000L, 30_000L);
    }
}
