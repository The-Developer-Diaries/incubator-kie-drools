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
package org.drools.grpc.session;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.kie.api.KieBase;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.KieSessionsPool;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.runtime.rule.LiveQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe session manager backed by {@link KieSessionsPool} for efficient
 * session reuse and resource management.
 * <p>
 * The pool pre-allocates sessions and recycles them on {@link KieSession#dispose()}.
 * For the stateless {@code Execute} RPC, sessions are borrowed from the pool, used,
 * and returned automatically. For stateful sessions created via {@code CreateSession},
 * the session is borrowed from the pool and tracked until the client calls
 * {@code DisposeSession}, at which point it is returned to the pool.
 */
public class SessionManager {

    private static final Logger log = LoggerFactory.getLogger(SessionManager.class);

    private static final int DEFAULT_POOL_SIZE = 10;

    private final KieSessionsPool sessionsPool;
    private final Map<String, KieSession> statefulSessions = new ConcurrentHashMap<>();
    private final Map<String, Map<String, FactHandle>> sessionFactHandles = new ConcurrentHashMap<>();
    private final Map<String, Thread> fireUntilHaltThreads = new ConcurrentHashMap<>();
    private final Map<String, List<LiveQuery>> sessionLiveQueries = new ConcurrentHashMap<>();

    /**
     * Creates a SessionManager backed by a pool from the given KieBase.
     *
     * @param kieBase  the KieBase to create sessions from
     * @param poolSize initial number of sessions to pre-allocate
     */
    public SessionManager(KieBase kieBase, int poolSize) {
        this.sessionsPool = kieBase.newKieSessionsPool(poolSize);
        log.info("Session pool initialized with size {}", poolSize);
    }

    public SessionManager(KieBase kieBase) {
        this(kieBase, DEFAULT_POOL_SIZE);
    }

    /**
     * Borrows a session from the pool for short-lived (stateless) use.
     * The caller MUST call {@link KieSession#dispose()} when done,
     * which returns the session to the pool rather than destroying it.
     */
    public KieSession borrowSession() {
        return sessionsPool.newKieSession();
    }

    /**
     * Creates a named stateful session from the pool.
     * The session is tracked until {@link #disposeSession(String)} is called.
     *
     * @param requestedId optional session ID; auto-generated if null or empty
     * @return the assigned session ID
     */
    public String createSession(String requestedId) {
        String sessionId = (requestedId != null && !requestedId.isEmpty())
                ? requestedId
                : UUID.randomUUID().toString();

        if (statefulSessions.containsKey(sessionId)) {
            throw new IllegalArgumentException("Session already exists: " + sessionId);
        }

        KieSession session = sessionsPool.newKieSession();
        statefulSessions.put(sessionId, session);
        sessionFactHandles.put(sessionId, new ConcurrentHashMap<>());
        log.debug("Stateful session created: {}", sessionId);
        return sessionId;
    }

    public KieSession getSession(String sessionId) {
        KieSession session = statefulSessions.get(sessionId);
        if (session == null) {
            throw new IllegalArgumentException("Session not found: " + sessionId);
        }
        return session;
    }

    public void trackFactHandle(String sessionId, String handleId, FactHandle factHandle) {
        Map<String, FactHandle> handles = sessionFactHandles.get(sessionId);
        if (handles != null) {
            handles.put(handleId, factHandle);
        }
    }

    public FactHandle getFactHandle(String sessionId, String handleId) {
        Map<String, FactHandle> handles = sessionFactHandles.get(sessionId);
        if (handles == null) {
            throw new IllegalArgumentException("Session not found: " + sessionId);
        }
        FactHandle fh = handles.get(handleId);
        if (fh == null) {
            throw new IllegalArgumentException("Fact handle not found: " + handleId);
        }
        return fh;
    }

    public void removeFactHandle(String sessionId, String handleId) {
        Map<String, FactHandle> handles = sessionFactHandles.get(sessionId);
        if (handles != null) {
            handles.remove(handleId);
        }
    }

    // --- Fire-until-halt management ---

    public void startFireUntilHalt(String sessionId) {
        KieSession session = getSession(sessionId);
        if (fireUntilHaltThreads.containsKey(sessionId)) {
            throw new IllegalStateException("Fire-until-halt already active for session: " + sessionId);
        }
        Thread thread = new Thread(session::fireUntilHalt, "fire-until-halt-" + sessionId);
        thread.setDaemon(true);
        fireUntilHaltThreads.put(sessionId, thread);
        thread.start();
        log.debug("Fire-until-halt started for session {}", sessionId);
    }

    public void halt(String sessionId) {
        KieSession session = getSession(sessionId);
        session.halt();
        Thread thread = fireUntilHaltThreads.remove(sessionId);
        if (thread != null) {
            try {
                thread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.debug("Halted session {}", sessionId);
    }

    public boolean isFireUntilHaltActive(String sessionId) {
        return fireUntilHaltThreads.containsKey(sessionId);
    }

    // --- Live query tracking ---

    public void trackLiveQuery(String sessionId, LiveQuery liveQuery) {
        sessionLiveQueries.computeIfAbsent(sessionId, k -> new CopyOnWriteArrayList<>()).add(liveQuery);
    }

    public void closeAllLiveQueries(String sessionId) {
        List<LiveQuery> queries = sessionLiveQueries.remove(sessionId);
        if (queries != null) {
            for (LiveQuery q : queries) {
                try {
                    q.close();
                } catch (Exception e) {
                    log.warn("Error closing live query for session {}", sessionId, e);
                }
            }
        }
    }

    public long getFactCount(String sessionId) {
        return getSession(sessionId).getFactCount();
    }

    /**
     * Disposes the stateful session and returns it to the pool.
     * Halts fire-until-halt and closes live queries if active.
     *
     * @return true if the session existed and was disposed
     */
    public boolean disposeSession(String sessionId) {
        if (fireUntilHaltThreads.containsKey(sessionId)) {
            halt(sessionId);
        }
        closeAllLiveQueries(sessionId);
        KieSession session = statefulSessions.remove(sessionId);
        sessionFactHandles.remove(sessionId);
        if (session != null) {
            session.dispose();
            log.debug("Stateful session disposed (returned to pool): {}", sessionId);
            return true;
        }
        return false;
    }

    /**
     * Shuts down the pool and releases all resources.
     * Called during server shutdown.
     */
    public void shutdown() {
        new ArrayList<>(fireUntilHaltThreads.keySet()).forEach(id -> {
            try {
                halt(id);
            } catch (Exception e) {
                log.warn("Error halting session {} during shutdown", id, e);
            }
        });
        new ArrayList<>(sessionLiveQueries.keySet()).forEach(this::closeAllLiveQueries);
        statefulSessions.forEach((id, session) -> {
            try {
                session.dispose();
            } catch (Exception e) {
                log.warn("Error disposing session {} during shutdown", id, e);
            }
        });
        statefulSessions.clear();
        sessionFactHandles.clear();
        sessionsPool.shutdown();
        log.info("Session pool shut down");
    }

    public int getActiveStatefulSessionCount() {
        return statefulSessions.size();
    }
}