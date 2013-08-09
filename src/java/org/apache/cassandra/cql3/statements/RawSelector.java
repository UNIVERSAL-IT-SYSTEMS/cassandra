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
package org.apache.cassandra.cql3.statements;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.RowMeta;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;

public interface RawSelector extends IVersionedSerializer<RawSelector>
{

    public static class RowMetaSelector implements RawSelector {
        public final ColumnIdentifier id = RowMeta.ROW_META_COLUMN;

        @Override
        public void serialize(RawSelector rawSelector, DataOutput out, int version) throws IOException {
            out.write(3);//type 3
        }

        @Override
        public RawSelector deserialize(DataInput in, int version) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long serializedSize(RawSelector rawSelector, int version) {
            return TypeSizes.NATIVE.sizeof(3);//type
        }

        @Override
        public String toString() {
            return id.toString();
        }
    }
    public static class WritetimeOrTTL implements RawSelector
    {
        public static final IVersionedSerializer<RawSelector> serializer = new WritetimeOrTTL(null, false);
        public final ColumnIdentifier id;
        public final boolean isWritetime;

        public WritetimeOrTTL(ColumnIdentifier id, boolean isWritetime)
        {
            this.id = id;
            this.isWritetime = isWritetime;
        }

        @Override
        public String toString()
        {
            return (isWritetime ? "writetime" : "ttl") + "(" + id + ")";
        }

        @Override
        public void serialize(RawSelector rawSelector, DataOutput out, int version) throws IOException {
            WritetimeOrTTL w = (WritetimeOrTTL) rawSelector;
            //type is 2
            out.write(2);
            w.id.serialize(rawSelector, out, version);
            out.writeBoolean(w.isWritetime);
        }

        public static WritetimeOrTTL from(DataInput in, int version) throws IOException {
            ColumnIdentifier id = ColumnIdentifier.from(in, version);
            boolean isWritetime = in.readBoolean();
            return new WritetimeOrTTL(id, isWritetime);
        }

        @Override
        public RawSelector deserialize(DataInput in, int version) throws IOException {
            return from(in, version);
        }

        @Override
        public long serializedSize(RawSelector rawSelector, int version) {
            WritetimeOrTTL w = (WritetimeOrTTL) rawSelector;
            long size = TypeSizes.NATIVE.sizeof(2);//type
            size += w.id.serializedSize(w, version);
            size += TypeSizes.NATIVE.sizeof(w.isWritetime);
            return size;
        }
    }

    public static class WithFunction implements RawSelector
    {
        public final String functionName;
        public final List<RawSelector> args;

        public WithFunction(String functionName, List<RawSelector> args)
        {
            this.functionName = functionName;
            this.args = args;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(functionName).append("(");
            for (int i = 0; i < args.size(); i++)
            {
                if (i > 0) sb.append(", ");
                sb.append(args.get(i));
            }
            return sb.append(")").toString();
        }

        @Override
        public void serialize(RawSelector rawSelector, DataOutput out, int version) throws IOException {
            WithFunction withFunction = (WithFunction) rawSelector;
            out.writeInt(1);//type 1
            out.writeUTF(withFunction.functionName);
            int argsSize = withFunction.args == null ? 0 : withFunction.args.size();
            out.writeInt(argsSize);
            for (RawSelector selector : withFunction.args) {
                selector.serialize(selector, out, version);
            }
        }

        public static WithFunction from(DataInput in, int version) throws IOException {
            String functionName = in.readUTF();
            int argsSize = in.readInt();
            List<RawSelector> args = new ArrayList<RawSelector>(argsSize);
            if (argsSize > 0) {
                for (int i = 0; i < argsSize; i++) {
                    RawSelector selector = serializer.deserialize(in, version);
                    args.add(selector);
                }
            }
            return new WithFunction(functionName, args);
        }

        @Override
        public RawSelector deserialize(DataInput in, int version) throws IOException {
            return from(in, version);
        }

        @Override
        public long serializedSize(RawSelector rawSelector, int version) {
            WithFunction w = (WithFunction) rawSelector;
            long size = TypeSizes.NATIVE.sizeof(1);//type
            size += TypeSizes.NATIVE.sizeof(w.functionName);
            int argsSize = w.args == null ? 0 : w.args.size();
            size += TypeSizes.NATIVE.sizeof(argsSize);
            if (argsSize > 0) {
                for (RawSelector selector : w.args) {
                    selector.serializedSize(selector, version);
                }
            }
            return size;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof WithFunction))
                return false;
            WithFunction other = (WithFunction) obj;
            if (other.functionName.equals(this.functionName)) {
                if (args.equals(other.args))
                    return true;
            }
            return false;
        }
    }

    public static RawSelectorSerializer serializer = new RawSelectorSerializer();

    public static class RawSelectorSerializer implements IVersionedSerializer<RawSelector> {

        @Override
        public void serialize(RawSelector rawSelector, DataOutput out, int version) throws IOException {
            rawSelector.serialize(rawSelector, out, version);
        }

        @Override
        public RawSelector deserialize(DataInput in, int version) throws IOException {
            int type = in.readInt();
            switch (type) {
                case 0:
                    return ColumnIdentifier.from(in, version);
                case 1:
                    return WithFunction.from(in, version);
                case 2:
                    return WritetimeOrTTL.from(in, version);
                case 3:
                    return new RowMetaSelector();
                default:
                    return null;
            }
        }

        @Override
        public long serializedSize(RawSelector rawSelector, int version) {
            return rawSelector.serializedSize(rawSelector,version);
        }
    }

}
