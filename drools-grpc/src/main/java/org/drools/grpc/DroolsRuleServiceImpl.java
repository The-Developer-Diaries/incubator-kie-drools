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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.grpc.stub.StreamObserver;
import org.drools.grpc.proto.CreateSessionRequest;
import org.drools.grpc.proto.CreateSessionResponse;
import org.drools.grpc.proto.DeleteFactRequest;
import org.drools.grpc.proto.DeleteFactResponse;
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
import org.drools.grpc.session.SessionManager;
import org.drools.grpc.util.FactConverter;
import org.kie.api.KieBase;
import org.kie.api.event.rule.AfterMatchFiredEvent;
import org.kie.api.event.rule.AgendaEventListener;
import org.kie.api.event.rule.AgendaGroupPoppedEvent;
import org.kie.api.event.rule.AgendaGroupPushedEvent;
import org.kie.api.event.rule.BeforeMatchFiredEvent;
import org.kie.api.event.rule.MatchCancelledEvent;
import org.kie.api.event.rule.MatchCreatedEvent;
import org.kie.api.event.rule.RuleFlowGroupActivatedEvent;
import org.kie.api.event.rule.RuleFlowGroupDeactivatedEvent;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC service implementation that delegates rule evaluation to a Drools {@link KieBase}.
 * Uses {@link org.kie.api.runtime.KieSessionsPool} via {@link SessionManager} for
 * efficient session reuse. Sessions are borrowed from the pool for stateless execution
 * and returned automatically on dispose.
 */
public class DroolsRuleServiceImpl extends DroolsRuleServiceGrpc.DroolsRuleServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(DroolsRuleServiceImpl.class);

    private final SessionManager sessionManager;
    private final FactConverter factConverter;

    public DroolsRuleServiceImpl(KieBase kieBase) {
        this(new SessionManager(kieBase), new FactConverter());
    }

    public DroolsRuleServiceImpl(SessionManager sessionManager, FactConverter factConverter) {
        this.sessionManager = sessionManager;
        this.factConverter = factConverter;
    }

    // --- Stateless execution ---

    @Override
    public void execute(ExecuteRequest request, StreamObserver<ExecuteResponse> responseObserver) {
        try {
            KieSession session = sessionManager.borrowSession();
            try {
                List<String> firedRuleNames = new ArrayList<>();
                session.addEventListener(ruleNameCollector(firedRuleNames));

                for (Fact fact : request.getFactsList()) {
                    Object obj = factConverter.toObject(fact);
                    session.insert(obj);
                }

                int maxRules = request.getMaxRules();
                int rulesFired = (maxRules > 0) ? session.fireAllRules(maxRules) : session.fireAllRules();

                List<Fact> resultFacts = new ArrayList<>();
                for (Object obj : session.getObjects()) {
                    resultFacts.add(factConverter.toFact(obj));
                }

                responseObserver.onNext(ExecuteResponse.newBuilder()
                        .setSuccess(true)
                        .setRulesFired(rulesFired)
                        .addAllResultFacts(resultFacts)
                        .addAllRulesFiredNames(firedRuleNames)
                        .build());
            } finally {
                session.dispose();
            }
        } catch (Exception e) {
            log.error("Execute failed", e);
            responseObserver.onNext(ExecuteResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage())
                    .build());
        }
        responseObserver.onCompleted();
    }

    // --- Session lifecycle ---

    @Override
    public void createSession(CreateSessionRequest request, StreamObserver<CreateSessionResponse> responseObserver) {
        try {
            String sessionId = sessionManager.createSession(request.getSessionId());
            log.info("Session created: {}", sessionId);
            responseObserver.onNext(CreateSessionResponse.newBuilder()
                    .setSuccess(true)
                    .setSessionId(sessionId)
                    .build());
        } catch (Exception e) {
            log.error("CreateSession failed", e);
            responseObserver.onNext(CreateSessionResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage())
                    .build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void disposeSession(DisposeSessionRequest request, StreamObserver<DisposeSessionResponse> responseObserver) {
        try {
            boolean disposed = sessionManager.disposeSession(request.getSessionId());
            if (disposed) {
                log.info("Session disposed: {}", request.getSessionId());
            }
            responseObserver.onNext(DisposeSessionResponse.newBuilder()
                    .setSuccess(disposed)
                    .setErrorMessage(disposed ? "" : "Session not found: " + request.getSessionId())
                    .build());
        } catch (Exception e) {
            log.error("DisposeSession failed", e);
            responseObserver.onNext(DisposeSessionResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage())
                    .build());
        }
        responseObserver.onCompleted();
    }

    // --- Fact operations ---

    @Override
    public void insertFact(InsertFactRequest request, StreamObserver<InsertFactResponse> responseObserver) {
        try {
            KieSession session = sessionManager.getSession(request.getSessionId());
            Object obj = factConverter.toObject(request.getFact());
            FactHandle handle = session.insert(obj);
            String handleId = handle.toExternalForm();
            sessionManager.trackFactHandle(request.getSessionId(), handleId, handle);

            responseObserver.onNext(InsertFactResponse.newBuilder()
                    .setSuccess(true)
                    .setFactHandleId(handleId)
                    .build());
        } catch (Exception e) {
            log.error("InsertFact failed", e);
            responseObserver.onNext(InsertFactResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage())
                    .build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void deleteFact(DeleteFactRequest request, StreamObserver<DeleteFactResponse> responseObserver) {
        try {
            KieSession session = sessionManager.getSession(request.getSessionId());
            FactHandle handle = sessionManager.getFactHandle(request.getSessionId(), request.getFactHandleId());
            session.delete(handle);
            sessionManager.removeFactHandle(request.getSessionId(), request.getFactHandleId());

            responseObserver.onNext(DeleteFactResponse.newBuilder()
                    .setSuccess(true)
                    .build());
        } catch (Exception e) {
            log.error("DeleteFact failed", e);
            responseObserver.onNext(DeleteFactResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage())
                    .build());
        }
        responseObserver.onCompleted();
    }

    // --- Rule firing ---

    @Override
    public void fireAllRules(FireAllRulesRequest request, StreamObserver<FireAllRulesResponse> responseObserver) {
        try {
            KieSession session = sessionManager.getSession(request.getSessionId());
            List<String> firedRuleNames = new ArrayList<>();
            AgendaEventListener collector = ruleNameCollector(firedRuleNames);
            session.addEventListener(collector);

            try {
                int maxRules = request.getMaxRules();
                int rulesFired = (maxRules > 0) ? session.fireAllRules(maxRules) : session.fireAllRules();

                responseObserver.onNext(FireAllRulesResponse.newBuilder()
                        .setSuccess(true)
                        .setRulesFired(rulesFired)
                        .addAllRulesFiredNames(firedRuleNames)
                        .build());
            } finally {
                session.removeEventListener(collector);
            }
        } catch (Exception e) {
            log.error("FireAllRules failed", e);
            responseObserver.onNext(FireAllRulesResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage())
                    .build());
        }
        responseObserver.onCompleted();
    }

    // --- Query facts ---

    @Override
    public void getFacts(GetFactsRequest request, StreamObserver<GetFactsResponse> responseObserver) {
        try {
            KieSession session = sessionManager.getSession(request.getSessionId());
            Collection<?> objects;
            if (request.getFactType() != null && !request.getFactType().isEmpty()) {
                Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(request.getFactType());
                objects = session.getObjects(obj -> clazz.isInstance(obj));
            } else {
                objects = session.getObjects();
            }

            List<Fact> facts = new ArrayList<>();
            for (Object obj : objects) {
                facts.add(factConverter.toFact(obj));
            }

            responseObserver.onNext(GetFactsResponse.newBuilder()
                    .setSuccess(true)
                    .addAllFacts(facts)
                    .build());
        } catch (Exception e) {
            log.error("GetFacts failed", e);
            responseObserver.onNext(GetFactsResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage())
                    .build());
        }
        responseObserver.onCompleted();
    }

    // --- Helpers ---

    private static AgendaEventListener ruleNameCollector(List<String> names) {
        return new AgendaEventListener() {
            @Override public void afterMatchFired(AfterMatchFiredEvent event) {
                names.add(event.getMatch().getRule().getName());
            }
            @Override public void matchCreated(MatchCreatedEvent event) { }
            @Override public void matchCancelled(MatchCancelledEvent event) { }
            @Override public void beforeMatchFired(BeforeMatchFiredEvent event) { }
            @Override public void agendaGroupPopped(AgendaGroupPoppedEvent event) { }
            @Override public void agendaGroupPushed(AgendaGroupPushedEvent event) { }
            @Override public void beforeRuleFlowGroupActivated(RuleFlowGroupActivatedEvent event) { }
            @Override public void afterRuleFlowGroupActivated(RuleFlowGroupActivatedEvent event) { }
            @Override public void beforeRuleFlowGroupDeactivated(RuleFlowGroupDeactivatedEvent event) { }
            @Override public void afterRuleFlowGroupDeactivated(RuleFlowGroupDeactivatedEvent event) { }
        };
    }
}