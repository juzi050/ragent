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

package com.nageoffer.ai.ragent.rag.rewrite;

import com.nageoffer.ai.ragent.framework.convention.ChatRequest;
import com.nageoffer.ai.ragent.infra.chat.LLMService;
import com.nageoffer.ai.ragent.infra.enums.Tier;
import com.nageoffer.ai.ragent.rag.config.RAGConfigProperties;
import com.nageoffer.ai.ragent.rag.core.prompt.PromptTemplateLoader;
import com.nageoffer.ai.ragent.rag.core.rewrite.MultiQuestionRewriteService;
import com.nageoffer.ai.ragent.rag.core.rewrite.QueryTermMappingService;
import com.nageoffer.ai.ragent.rag.core.rewrite.RewriteResult;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MultiQuestionRewriteServiceUnitTest {

    @Test
    void shouldDeduplicateLlmSubQuestionsInEncounterOrder() {
        LLMService llmService = mock(LLMService.class);
        RAGConfigProperties properties = mock(RAGConfigProperties.class);
        QueryTermMappingService mappingService = mock(QueryTermMappingService.class);
        PromptTemplateLoader templateLoader = mock(PromptTemplateLoader.class);
        MultiQuestionRewriteService service = new MultiQuestionRewriteService(
                llmService, properties, mappingService, templateLoader);

        when(properties.getQueryRewriteEnabled()).thenReturn(true);
        when(mappingService.normalize("订单流程和支付流程")).thenReturn("订单流程和支付流程");
        when(templateLoader.load(any())).thenReturn("rewrite prompt");
        when(llmService.chat(any(ChatRequest.class), eq(Tier.FAST))).thenReturn("""
                {
                  "rewrite": "订单流程和支付流程",
                  "should_split": true,
                  "sub_questions": [
                    "订单流程是什么",
                    "订单流程是什么",
                    "支付流程怎么处理"
                  ]
                }
                """);

        RewriteResult result = service.rewriteWithSplit("订单流程和支付流程");

        assertEquals(List.of("订单流程是什么", "支付流程怎么处理"), result.subQuestions());
    }

    @Test
    void shouldDeduplicateRuleBasedSubQuestionsInEncounterOrder() {
        LLMService llmService = mock(LLMService.class);
        RAGConfigProperties properties = mock(RAGConfigProperties.class);
        QueryTermMappingService mappingService = mock(QueryTermMappingService.class);
        PromptTemplateLoader templateLoader = mock(PromptTemplateLoader.class);
        MultiQuestionRewriteService service = new MultiQuestionRewriteService(
                llmService, properties, mappingService, templateLoader);

        String question = "订单流程？订单流程？支付流程？";
        when(properties.getQueryRewriteEnabled()).thenReturn(false);
        when(mappingService.normalize(question)).thenReturn(question);

        RewriteResult result = service.rewriteWithSplit(question);

        assertEquals(List.of("订单流程？", "支付流程？"), result.subQuestions());
    }
}
