package org.apache.cassandra.transport.messages;

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.commons.lang.StringUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.lang.reflect.Method;
import java.util.UUID;

/**
 * User: satya
 */
public class QueryMessage2 extends QueryMessage {

    public QueryMessage2(String query, ConsistencyLevel consistency) {
        super(query, consistency);
    }

    static Object xQueryProcessor;
    static Class xQueryProcessorClass;
    static boolean xQueryProcessorInited;

    static {
        try {
            xQueryProcessorClass = Class.forName("com.tuplejump.stargate.cas.engine.XQueryProcessor");
            xQueryProcessor = xQueryProcessorClass.newInstance();
            xQueryProcessorInited = true;
        } catch (Exception e) {
            logger.error("com.tuplejump.stargate.cas.engine.XQueryProcessor could not be initialized", e);
            xQueryProcessorInited = false;
        }
    }

    public static final Message.Codec<QueryMessage2> codec = new Message.Codec<QueryMessage2>() {
        public QueryMessage2 decode(ChannelBuffer body) {
            String query = CBUtil.readLongString(body);
            ConsistencyLevel consistency = CBUtil.readConsistencyLevel(body);
            return new QueryMessage2(query, consistency);
        }

        public ChannelBuffer encode(QueryMessage2 msg) {
            return ChannelBuffers.wrappedBuffer(CBUtil.longStringToCB(msg.query), CBUtil.consistencyLevelToCB(msg.consistency));
        }
    };

    public ChannelBuffer encode() {
        return codec.encode(this);
    }

    public Message.Response execute(QueryState state) {
        boolean xcql = false;
        if (StringUtils.containsIgnoreCase(query, "XSELECT") || StringUtils.containsIgnoreCase(query, "XCREATE")) {
            xcql = true;
        }
        try {
            UUID tracingId = null;
            if (isTracingRequested()) {
                tracingId = UUIDGen.getTimeUUID();
                state.prepareTracingSession(tracingId);
            }

            if (state.traceNextQuery()) {
                state.createTracingSession();
                if (xcql) {
                    Tracing.instance().begin("Execute XCQL query", ImmutableMap.of("query", query));
                } else {
                    Tracing.instance().begin("Execute CQL3 query", ImmutableMap.of("query", query));
                }
            }

            Message.Response response;
            if (xcql) {
                if (!xQueryProcessorInited)
                    throw new InvalidRequestException("com.tuplejump.stargate.cas.engine.XQueryProcessor was not initliazed properly");
                else {
                    try {
                        Method processM = xQueryProcessorClass.getMethod("processXQuery", String.class, ConsistencyLevel.class, QueryState.class);
                        response = (Response) processM.invoke(null, query, consistency, state);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        logger.error("Error occured in executing xquery", e.getTargetException());
                        throw new InvalidRequestException(e.getTargetException().getMessage());
                    }
                }
            } else {
                response = QueryProcessor.process(query, consistency, state);
            }

            if (tracingId != null)
                response.setTracingId(tracingId);

            return response;
        } catch (Exception e) {
            if (!((e instanceof RequestValidationException) || (e instanceof RequestExecutionException)))
                logger.error("Unexpected error during query", e);
            return ErrorMessage.fromException(e);
        } finally {
            Tracing.instance().stopSession();
        }
    }

    @Override
    public String toString() {
        return "QUERY " + query;
    }

}
