/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.drools.grpc;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import org.drools.grpc.proto.CreateSessionRequest;
import org.drools.grpc.proto.CreateSessionResponse;
import org.drools.grpc.proto.DisposeSessionRequest;
import org.drools.grpc.proto.DisposeSessionResponse;
import org.drools.grpc.proto.ExecuteRequest;
import org.drools.grpc.proto.ExecuteResponse;
import org.drools.grpc.proto.Fact;
import org.drools.grpc.proto.FireAllRulesRequest;
import org.drools.grpc.proto.FireAllRulesResponse;
import org.drools.grpc.proto.GetFactsRequest;
import org.drools.grpc.proto.GetFactsResponse;
import org.drools.grpc.proto.InsertFactRequest;
import org.drools.grpc.proto.InsertFactResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.Message;
import org.kie.api.runtime.KieContainer;

import static org.assertj.core.api.Assertions.assertThat;

class DroolsRuleServiceImplTest {

    private Server server;
    private ManagedChannel channel;

    @BeforeEach
    void setUp() throws Exception {
        KieBase kieBase = createKieBase();
        String serverName = InProcessServerBuilder.generateName();

        server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new DroolsRuleServiceImpl(kieBase))
                .build()
                .start();

        channel = InProcessChannelBuilder.forName(serverName)
                .directExecutor()
                .build();
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void shouldExecuteStatelessRules() {
        Fact orderFact = Fact.newBuilder()
                .setType("org.drools.grpc.TestOrder")
                .setJson("{\"amount\": 150.0, \"discount\": 0.0}")
                .build();

        ExecuteRequest request = ExecuteRequest.newBuilder()
                .addFacts(orderFact)
                .build();

        ExecuteResponse response = blockingUnary(
                DroolsRuleServiceGrpc.EXECUTE_METHOD, request);

        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getRulesFired()).isGreaterThan(0);
        assertThat(response.getRulesFiredNamesList()).contains("Apply Discount");
    }

    @Test
    void shouldManageStatefulSessions() {
        // Create session
        CreateSessionResponse createResp = blockingUnary(
                DroolsRuleServiceGrpc.CREATE_SESSION_METHOD,
                CreateSessionRequest.newBuilder().setSessionId("test-session").build());
        assertThat(createResp.getSuccess()).isTrue();
        assertThat(createResp.getSessionId()).isEqualTo("test-session");

        // Insert fact
        Fact orderFact = Fact.newBuilder()
                .setType("org.drools.grpc.TestOrder")
                .setJson("{\"amount\": 200.0, \"discount\": 0.0}")
                .build();
        InsertFactResponse insertResp = blockingUnary(
                DroolsRuleServiceGrpc.INSERT_FACT_METHOD,
                InsertFactRequest.newBuilder()
                        .setSessionId("test-session")
                        .setFact(orderFact)
                        .build());
        assertThat(insertResp.getSuccess()).isTrue();
        assertThat(insertResp.getFactHandleId()).isNotEmpty();

        // Fire rules
        FireAllRulesResponse fireResp = blockingUnary(
                DroolsRuleServiceGrpc.FIRE_ALL_RULES_METHOD,
                FireAllRulesRequest.newBuilder()
                        .setSessionId("test-session")
                        .build());
        assertThat(fireResp.getSuccess()).isTrue();
        assertThat(fireResp.getRulesFired()).isGreaterThan(0);

        // Get facts
        GetFactsResponse getResp = blockingUnary(
                DroolsRuleServiceGrpc.GET_FACTS_METHOD,
                GetFactsRequest.newBuilder()
                        .setSessionId("test-session")
                        .build());
        assertThat(getResp.getSuccess()).isTrue();
        assertThat(getResp.getFactsList()).isNotEmpty();

        // Dispose session
        DisposeSessionResponse disposeResp = blockingUnary(
                DroolsRuleServiceGrpc.DISPOSE_SESSION_METHOD,
                DisposeSessionRequest.newBuilder()
                        .setSessionId("test-session")
                        .build());
        assertThat(disposeResp.getSuccess()).isTrue();
    }

    @Test
    void shouldReturnErrorForInvalidSession() {
        FireAllRulesResponse response = blockingUnary(
                DroolsRuleServiceGrpc.FIRE_ALL_RULES_METHOD,
                FireAllRulesRequest.newBuilder()
                        .setSessionId("nonexistent")
                        .build());
        assertThat(response.getSuccess()).isFalse();
        assertThat(response.getErrorMessage()).contains("nonexistent");
    }

    @Test
    void shouldReturnErrorForInvalidFactType() {
        Fact badFact = Fact.newBuilder()
                .setType("com.nonexistent.Foo")
                .setJson("{}")
                .build();
        ExecuteResponse response = blockingUnary(
                DroolsRuleServiceGrpc.EXECUTE_METHOD,
                ExecuteRequest.newBuilder().addFacts(badFact).build());
        assertThat(response.getSuccess()).isFalse();
        assertThat(response.getErrorMessage()).contains("nonexistent");
    }

    // --- Helpers ---

    private <ReqT, RespT> RespT blockingUnary(
            io.grpc.MethodDescriptor<ReqT, RespT> method, ReqT request) {
        return ClientCalls.blockingUnaryCall(channel, method,
                io.grpc.CallOptions.DEFAULT, request);
    }

    private static KieBase createKieBase() {
        KieServices ks = KieServices.Factory.get();
        KieFileSystem kfs = ks.newKieFileSystem();
        kfs.write("src/main/resources/rules/test.drl",
                "package org.drools.grpc;\n" +
                        "\n" +
                        "import org.drools.grpc.TestOrder;\n" +
                        "\n" +
                        "rule \"Apply Discount\"\n" +
                        "when\n" +
                        "    $order : TestOrder(amount > 100, discount == 0.0)\n" +
                        "then\n" +
                        "    $order.setDiscount($order.getAmount() * 0.1);\n" +
                        "    update($order);\n" +
                        "end\n");

        KieBuilder kieBuilder = ks.newKieBuilder(kfs).buildAll();
        if (kieBuilder.getResults().hasMessages(Message.Level.ERROR)) {
            throw new RuntimeException("Rule compilation errors: " + kieBuilder.getResults().getMessages());
        }
        KieContainer kieContainer = ks.newKieContainer(ks.getRepository().getDefaultReleaseId());
        return kieContainer.getKieBase();
    }
}