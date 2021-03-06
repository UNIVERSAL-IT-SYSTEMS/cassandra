/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.streaming;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.FBUtilities;


/**
 * A future on the result ({@link StreamState}) of a streaming plan.
 *
 * In practice, this object also groups all the {@link StreamSession} for the streaming job
 * involved. One StreamSession will be created for every peer involved and said session will
 * handle every streaming (outgoing and incoming) to that peer for this job.
 * <p>
 * The future will return a result once every session is completed (successfully or not). If
 * any session ended up with an error, the future will throw a StreamException.
 * <p>
 * You can attach {@link StreamEventHandler} to this object to listen on {@link StreamEvent}s to
 * track progress of the streaming.
 */
public final class StreamResultFuture extends AbstractFuture<StreamState>
{
    private static final Logger logger = LoggerFactory.getLogger(StreamResultFuture.class);

    public final UUID planId;
    public final String description;
    private final List<StreamEventHandler> eventListeners = Collections.synchronizedList(new ArrayList<StreamEventHandler>());

    private final Map<InetAddress, StreamSession> ongoingSessions;
    private final Map<InetAddress, SessionInfo> sessionStates = new NonBlockingHashMap<>();

    /**
     * Create new StreamResult of given {@code planId} and type.
     *
     * Constructor is package private. You need to use {@link StreamPlan#execute()} to get the instance.
     *
     * @param planId Stream plan ID
     * @param description Stream description
     * @param numberOfSessions number of sessions to wait for complete
     */
    private StreamResultFuture(UUID planId, String description, Collection<StreamSession> sessions)
    {
        this.planId = planId;
        this.description = description;
        this.ongoingSessions = new HashMap<>(sessions.size());
        for (StreamSession session : sessions)
            this.ongoingSessions.put(session.peer, session);;

        // if there is no session to listen to, we immediately set result for returning
        if (sessions.isEmpty())
            set(getCurrentState());
    }

    static StreamResultFuture init(UUID planId, String description, Collection<StreamSession> sessions)
    {
        StreamResultFuture future = createAndRegister(planId, description, sessions);

        // start sessions
        for (final StreamSession session : sessions)
        {
            session.init(future);
            session.start();
        }

        return future;
    }

    public static StreamResultFuture initReceivingSide(UUID planId, String description, InetAddress from, Socket socket, int version)
    {
        final StreamSession session = new StreamSession(from);

        // The main reason we create a StreamResultFuture on the receiving side is for JMX exposure.
        StreamResultFuture future = createAndRegister(planId, description, Collections.singleton(session));

        session.init(future);
        session.start(socket, version);

        return future;
    }

    private static StreamResultFuture createAndRegister(UUID planId, String description, Collection<StreamSession> sessions)
    {
        StreamResultFuture future = new StreamResultFuture(planId, description, sessions);
        StreamManager.instance.register(future);
        return future;
    }

    public void startStreaming(InetAddress from, Socket socket, int version) throws IOException
    {
        StreamSession session = ongoingSessions.get(from);
        if (session == null)
            throw new RuntimeException(String.format("Got connection from %s for stream session %s but no such session locally", from, planId));

        session.handler.attachIncomingSocket(socket, version);
        session.onInitializationComplete();
    }

    public void addEventListener(StreamEventHandler listener)
    {
        Futures.addCallback(this, listener);
        eventListeners.add(listener);
    }

    /**
     * @return Current snapshot of streaming progress.
     */
    public StreamState getCurrentState()
    {
        return new StreamState(planId, description, ImmutableSet.copyOf(sessionStates.values()));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamResultFuture that = (StreamResultFuture) o;
        return planId.equals(that.planId);
    }

    @Override
    public int hashCode()
    {
        return planId.hashCode();
    }

    void handleSessionPrepared(StreamSession session)
    {
        SessionInfo sessionInfo = session.getSessionInfo();
        StreamEvent.SessionPreparedEvent event = new StreamEvent.SessionPreparedEvent(planId, sessionInfo);
        sessionStates.put(sessionInfo.peer, sessionInfo);
        fireStreamEvent(event);
    }

    void handleSessionComplete(StreamSession session)
    {
        logger.debug("Session with {} is complete", session.peer);

        SessionInfo sessionInfo = session.getSessionInfo();
        sessionStates.put(sessionInfo.peer, sessionInfo);
        fireStreamEvent(new StreamEvent.SessionCompleteEvent(session));
        maybeComplete(session);
    }

    public void handleProgress(ProgressInfo progress)
    {
        sessionStates.get(progress.peer).updateProgress(progress);
        fireStreamEvent(new StreamEvent.ProgressEvent(planId, progress));
    }

    void fireStreamEvent(StreamEvent event)
    {
        // delegate to listener
        for (StreamEventHandler listener : eventListeners)
            listener.handleStreamEvent(event);
    }

    private synchronized void maybeComplete(StreamSession session)
    {
        ongoingSessions.remove(session.peer);
        if (ongoingSessions.isEmpty())
        {
            StreamState finalState = getCurrentState();
            if (finalState.hasFailedSession())
                setException(new StreamException(finalState, "Stream failed"));
            else
                set(finalState);
        }
    }
}
