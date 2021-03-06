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
package org.apache.cassandra.transport.messages;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.UUIDGen;

public class BatchMessage extends Message.Request
{
    public static final Message.Codec<BatchMessage> codec = new Message.Codec<BatchMessage>()
    {
        public BatchMessage decode(ChannelBuffer body, int version)
        {
            if (version == 1)
                throw new ProtocolException("BATCH messages are not support in version 1 of the protocol");

            byte type = body.readByte();
            int n = body.readUnsignedShort();
            List<Object> queryOrIds = new ArrayList<Object>(n);
            List<List<ByteBuffer>> variables = new ArrayList<List<ByteBuffer>>(n);
            for (int i = 0; i < n; i++)
            {
                byte kind = body.readByte();
                if (kind == 0)
                    queryOrIds.add(CBUtil.readLongString(body));
                else if (kind == 1)
                    queryOrIds.add(MD5Digest.wrap(CBUtil.readBytes(body)));
                else
                    throw new ProtocolException("Invalid query kind in BATCH messages. Must be 0 or 1 but got " + kind);

                int count = body.readUnsignedShort();
                List<ByteBuffer> values = count == 0 ? Collections.<ByteBuffer>emptyList() : new ArrayList<ByteBuffer>(count);
                for (int j = 0; j < count; j++)
                    values.add(CBUtil.readValue(body));
                variables.add(values);
            }
            ConsistencyLevel consistency = CBUtil.readConsistencyLevel(body);
            return new BatchMessage(toType(type), queryOrIds, variables, consistency);
        }

        public ChannelBuffer encode(BatchMessage msg, int version)
        {
            // We have:
            //   - type
            //   - Number of queries
            //   - For each query:
            //      - kind
            //      - string or id
            //      - value count
            //      - values
            //   - consistency
            int queries = msg.queryOrIdList.size();
            int totalValues = count(msg.values);

            ChannelBuffer header = ChannelBuffers.buffer(3);
            header.writeByte(fromType(msg.type));
            header.writeShort(queries);

            CBUtil.BufferBuilder builder = new CBUtil.BufferBuilder(2 + queries * 3, 0, totalValues);
            builder.add(header);
            for (int i = 0; i < queries; i++)
            {
                Object q = msg.queryOrIdList.get(i);
                builder.add(CBUtil.byteToCB((byte)(q instanceof String ? 0 : 1)));
                if (q instanceof String)
                    builder.add(CBUtil.longStringToCB((String)q));
                else
                    builder.add(CBUtil.bytesToCB(((MD5Digest)q).bytes));
                List<ByteBuffer> queryValues = msg.values.get(i);
                builder.add(CBUtil.shortToCB(queryValues.size()));
                for (ByteBuffer value : queryValues)
                    builder.addValue(value);
            }

            builder.add(CBUtil.consistencyLevelToCB(msg.consistency));
            return builder.build();
        }

        private BatchStatement.Type toType(byte b)
        {
            if (b == 0)
                return BatchStatement.Type.LOGGED;
            else if (b == 1)
                return BatchStatement.Type.UNLOGGED;
            else if (b == 2)
                return BatchStatement.Type.COUNTER;
            else
                throw new ProtocolException("Invalid BATCH message type " + b);
        }

        private byte fromType(BatchStatement.Type type)
        {
            switch (type)
            {
                case LOGGED:   return 0;
                case UNLOGGED: return 1;
                case COUNTER:  return 2;
                default:
                    throw new AssertionError();
            }
        }

        private int count(List<List<ByteBuffer>> values)
        {
            int count = 0;
            for (List<ByteBuffer> l : values)
                count += l.size();
            return count;
        }
    };

    public final BatchStatement.Type type;
    public final List<Object> queryOrIdList;
    public final List<List<ByteBuffer>> values;
    public final ConsistencyLevel consistency;

    public BatchMessage(BatchStatement.Type type, List<Object> queryOrIdList, List<List<ByteBuffer>> values, ConsistencyLevel consistency)
    {
        super(Message.Type.BATCH);
        this.type = type;
        this.queryOrIdList = queryOrIdList;
        this.values = values;
        this.consistency = consistency;
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this, getVersion());
    }

    public Message.Response execute(QueryState state)
    {
        try
        {
            UUID tracingId = null;
            if (isTracingRequested())
            {
                tracingId = UUIDGen.getTimeUUID();
                state.prepareTracingSession(tracingId);
            }

            if (state.traceNextQuery())
            {
                state.createTracingSession();
                // TODO we don't have [typed] access to CQL bind variables here.  CASSANDRA-4560 is open to add support.
                Tracing.instance.begin("Execute batch of CQL3 queries", Collections.<String, String>emptyMap());
            }

            List<ModificationStatement> statements = new ArrayList<ModificationStatement>(queryOrIdList.size());
            for (int i = 0; i < queryOrIdList.size(); i++)
            {
                Object query = queryOrIdList.get(i);
                CQLStatement statement;
                if (query instanceof String)
                {
                    statement = QueryProcessor.parseStatement((String)query, state);
                }
                else
                {
                    statement = QueryProcessor.getPrepared((MD5Digest)query);
                    if (statement == null)
                        throw new PreparedQueryNotFoundException((MD5Digest)query);
                }

                List<ByteBuffer> queryValues = values.get(i);
                if (queryValues.size() != statement.getBoundsTerms())
                    throw new InvalidRequestException(String.format("There were %d markers(?) in CQL but %d bound variables",
                                                                    statement.getBoundsTerms(),
                                                                    queryValues.size()));
                if (!(statement instanceof ModificationStatement))
                    throw new InvalidRequestException("Invalid statement in batch: only UPDATE, INSERT and DELETE statements are allowed.");

                ModificationStatement mst = (ModificationStatement)statement;
                if (mst.isCounter())
                {
                    if (type != BatchStatement.Type.COUNTER)
                        throw new InvalidRequestException("Cannot include counter statement in a non-counter batch");
                }
                else
                {
                    if (type == BatchStatement.Type.COUNTER)
                        throw new InvalidRequestException("Cannot include non-counter statement in a counter batch");
                }
                statements.add(mst);
            }

            // Note: It's ok at this point to pass a bogus value for the number of bound terms in the BatchState ctor
            // (and no value would be really correct, so we prefer passing a clearly wrong one).
            BatchStatement batch = new BatchStatement(-1, type, statements, Attributes.none());
            Message.Response response = QueryProcessor.processBatch(batch, consistency, state, values);

            if (tracingId != null)
                response.setTracingId(tracingId);

            return response;
        }
        catch (Exception e)
        {
            return ErrorMessage.fromException(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("BATCH of [");
        for (int i = 0; i < queryOrIdList.size(); i++)
        {
            if (i > 0) sb.append(", ");
            sb.append(queryOrIdList.get(i)).append(" with ").append(values.get(i).size()).append(" values");
        }
        sb.append("] at consistency ").append(consistency);
        return sb.toString();
    }
}
