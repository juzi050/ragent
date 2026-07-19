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

package com.nageoffer.ai.ragent.rag.core.retrieval.channel;

import cn.hutool.core.collection.CollUtil;
import com.nageoffer.ai.ragent.framework.convention.RetrievedChunk;
import com.nageoffer.ai.ragent.rag.config.GraphProperties;
import com.nageoffer.ai.ragent.rag.config.SearchChannelProperties;
import com.nageoffer.ai.ragent.rag.core.graph.LightRagClient;
import com.nageoffer.ai.ragent.rag.core.intent.NodeScore;
import com.nageoffer.ai.ragent.rag.core.intent.NodeScoreFilters;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 知识图谱检索通道
 * <p>
 * 基于 LightRAG 的图谱召回：擅长多跳关系推理与实体为中心的聚合，与向量 / 关键词互补
 * 仅当开启图谱后端（rag.graph.type=lightrag）时才注册，否则整个通道不存在，引擎自动退化为无图谱检索
 * <p>
 * 优先级介于意图定向(1) 与关键词(5) 之间
 * <p>
 * 说明：LightRAG /query 无 per-request workspace，单实例即单图，本通道 Phase1 面向全局图召回；
 * 按 KB 隔离子图（借 file_path 归属过滤或多实例）留待后续阶段。mode=intent 时无 KB 意图则跳过，语义对齐关键词通道
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "rag.graph", name = "type", havingValue = "lightrag")
public class GraphSearchChannel implements SearchChannel {

    private static final String MODE_INTENT = "intent";
    private static final String MODE_GLOBAL = "global";

    /**
     * 过滤时向 LightRAG 的请求量上浮倍数
     * 结果侧过滤在 top_k 截断后再筛掉跨库证据，命中库证据可能变少，过滤时多取以补召回
     */
    private static final int FILTER_TOPK_BOOST = 3;

    private final LightRagClient lightRagClient;
    private final GraphProperties graphProperties;
    private final SearchChannelProperties properties;

    @Override
    public String getName() {
        return "GraphSearch";
    }

    @Override
    public SearchChannelType getType() {
        return SearchChannelType.GRAPH;
    }

    @Override
    public boolean isEnabled(SearchContext context) {
        return properties.getChannels().getGraph().isEnabled();
    }

    @Override
    public SearchChannelResult search(SearchContext context) {
        long startTime = System.currentTimeMillis();
        try {
            String mode = properties.getChannels().getGraph().getMode();
            List<String> intentCollections = extractIntentCollections(context);

            // intent 模式且无可定向的 KB 意图库则跳过，语义与关键词通道一致
            if (MODE_INTENT.equalsIgnoreCase(mode) && CollUtil.isEmpty(intentCollections)) {
                log.info("图谱检索为 intent 模式但无 KB 意图，跳过");
                return emptyResult(startTime);
            }

            // 按 mode 决定过滤范围：global 不过滤（空）/ intent、both 有意图库则收敛到意图域，否则全局兜底（空）
            List<String> collections = resolveCollections(mode, intentCollections);

            int topKMultiplier = properties.getChannels().getGraph().getTopKMultiplier();
            int baseTopK = context.getTopK() * Math.max(1, topKMultiplier);
            // 过滤生效时上浮请求量补召回；全局不过滤则用基础量
            int topK = CollUtil.isEmpty(collections) ? baseTopK : baseTopK * FILTER_TOPK_BOOST;
            String queryMode = graphProperties.getLightrag().getQueryMode();

            List<RetrievedChunk> chunks = lightRagClient.retrieve(context.getMainQuestion(), queryMode, topK, collections);

            long latency = System.currentTimeMillis() - startTime;
            log.info("图谱检索完成，范围={}，检索到 {} 个证据，耗时 {}ms",
                    CollUtil.isEmpty(collections) ? "全局" : collections, chunks.size(), latency);

            return SearchChannelResult.builder()
                    .channelType(SearchChannelType.GRAPH)
                    .channelName(getName())
                    .chunks(chunks)
                    .latencyMs(latency)
                    .build();
        } catch (Exception e) {
            log.error("图谱检索失败", e);
            return emptyResult(startTime);
        }
    }

    /**
     * 按 mode 解析过滤范围
     * global 不过滤（空）/ intent、both 直接用意图域（空则自然回退全局不过滤）
     */
    private List<String> resolveCollections(String mode, List<String> intentCollections) {
        if (MODE_GLOBAL.equalsIgnoreCase(mode)) {
            return List.of();
        }
        return intentCollections;
    }

    /**
     * 从意图识别结果提取 KB 意图对应的 collection 名称
     */
    private List<String> extractIntentCollections(SearchContext context) {
        if (CollUtil.isEmpty(context.getIntents())) {
            return List.of();
        }
        List<NodeScore> allScores = context.getIntents().stream()
                .flatMap(si -> si.nodeScores().stream())
                .toList();
        return NodeScoreFilters.kbCollections(allScores);
    }

    private SearchChannelResult emptyResult(long startTime) {
        return SearchChannelResult.builder()
                .channelType(SearchChannelType.GRAPH)
                .channelName(getName())
                .chunks(List.of())
                .latencyMs(System.currentTimeMillis() - startTime)
                .build();
    }
}
