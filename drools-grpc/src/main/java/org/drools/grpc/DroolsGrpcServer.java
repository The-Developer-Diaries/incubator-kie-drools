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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import org.drools.grpc.session.SessionManager;
import org.drools.grpc.util.FactConverter;
import org.kie.api.KieBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone gRPC server that exposes a Drools KieBase for remote rule evaluation.
 *
 * <p>Usage:
 * <pre>{@code
 *   KieBase kieBase = ...;
 *   DroolsGrpcServer server = DroolsGrpcServer.builder(kieBase)
 *       .port(50051)
 *       .build();
 *   server.start();
 *   server.blockUntilShutdown();
 * }</pre>
 */
public class DroolsGrpcServer {

    private static final Logger log = LoggerFactory.getLogger(DroolsGrpcServer.class);

    private final Server server;
    private final SessionManager sessionManager;

    private DroolsGrpcServer(Server server, SessionManager sessionManager) {
        this.server = server;
        this.sessionManager = sessionManager;
    }

    public static Builder builder(KieBase kieBase) {
        return new Builder(kieBase);
    }

    public void start() throws IOException {
        server.start();
        log.info("Drools gRPC server started on port {}", server.getPort());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down Drools gRPC server...");
            try {
                DroolsGrpcServer.this.stop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }

    public void stop() throws InterruptedException {
        sessionManager.shutdown();
        server.shutdown();
        if (!server.awaitTermination(30, TimeUnit.SECONDS)) {
            server.shutdownNow();
        }
        log.info("Drools gRPC server stopped");
    }

    public void blockUntilShutdown() throws InterruptedException {
        server.awaitTermination();
    }

    public int getPort() {
        return server.getPort();
    }

    public boolean isRunning() {
        return !server.isShutdown() && !server.isTerminated();
    }

    public static class Builder {

        private final KieBase kieBase;
        private int port = 50051;
        private int sessionPoolSize = 10;
        private boolean enableReflection = true;
        private FactConverter factConverter;
        private SessionManager sessionManager;

        private Builder(KieBase kieBase) {
            this.kieBase = kieBase;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Sets the initial size of the {@link org.kie.api.runtime.KieSessionsPool}.
         * Sessions are pre-allocated and reused across gRPC calls. Default: 10.
         */
        public Builder sessionPoolSize(int poolSize) {
            this.sessionPoolSize = poolSize;
            return this;
        }

        public Builder enableReflection(boolean enable) {
            this.enableReflection = enable;
            return this;
        }

        public Builder factConverter(FactConverter factConverter) {
            this.factConverter = factConverter;
            return this;
        }

        public Builder sessionManager(SessionManager sessionManager) {
            this.sessionManager = sessionManager;
            return this;
        }

        public DroolsGrpcServer build() {
            SessionManager sm = sessionManager != null
                    ? sessionManager
                    : new SessionManager(kieBase, sessionPoolSize);
            FactConverter fc = factConverter != null ? factConverter : new FactConverter();
            DroolsRuleServiceImpl service = new DroolsRuleServiceImpl(sm, fc);

            ServerBuilder<?> serverBuilder = ServerBuilder.forPort(port)
                    .addService(service);

            if (enableReflection) {
                serverBuilder.addService(ProtoReflectionService.newInstance());
            }

            return new DroolsGrpcServer(serverBuilder.build(), sm);
        }
    }
}