package org.apache.cassandra.db;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * User: satya
 * <p/>
 * A Map to contain row meta data.
 */
public class RowMeta extends LinkedHashMap<String, String> {
    protected static final Logger logger = LoggerFactory.getLogger(RowMeta.class);

    public static final ColumnIdentifier ROW_META_COLUMN = new ColumnIdentifier("xrowmeta", false);
    public static final RowMetaComparator ROW_META_TYPE = RowMetaComparator.getSerDeInstance();
    public static final String COLUMNS = "ROW_META_COLUMNS";
    public static final String ORDER_COLUMNS = "ROW_META_ORDER_COLUMNS";
    public static final String ORDER_TYPES = "ROW_META_ORDER_TYPES";
    public static final String ORDER_REVERSE = "ROW_META_ORDER_REV";

    public static final RowMetaSerializer serializer = new RowMetaSerializer();

    public AbstractType rowMetaComparator() {
        if (size() > 0 && get(ORDER_COLUMNS) != null && get(ORDER_TYPES) != null)
            return RowMetaComparator.getInstance(this);
        return null;
    }

    public RowMeta(int size) {
        super(size);
    }

    public RowMeta(Map<String, String> map) {
        super(map);
    }

    public static RowMeta fromJson(String json) {
        LinkedHashMap<String, String> map = new LinkedHashMap();
        String entries1 = json.replace("{", "");
        entries1 = entries1.replace("}", "");
        String[] entries = entries1.split(",");
        for (String entryStr : entries) {
            String[] keyVal = entryStr.split(":");
            map.put(keyVal[0], keyVal[1]);
        }
        return new RowMeta(map);
    }

    public String toJson(Map<String, String> map) {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        builder.append('{');
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (!first) {
                builder.append(',');
            }
            builder.append(entry.getKey()).append(':').append(entry.getValue());
            first = false;
        }
        builder.append('}');
        return builder.toString();
    }

    protected boolean isMeta(String key) {
        return key.equals(COLUMNS) || key.equals(ORDER_COLUMNS) || key.equals(ORDER_TYPES) || key.equals(ORDER_REVERSE);
    }

    public static class RowMetaSerializer implements IVersionedSerializer<RowMeta> {

        @Override
        public void serialize(RowMeta rowMeta, DataOutput dos, int version) throws IOException {
            if (logger.isDebugEnabled())
                logger.debug("Row meta serializing - " + rowMeta);
            if (rowMeta == null) {
                dos.writeInt(0);
            } else {
                dos.writeInt(rowMeta.size());
                for (Map.Entry<String, String> entry : rowMeta.entrySet()) {
                    dos.writeUTF(entry.getKey());
                    dos.writeUTF(entry.getValue());
                }
            }
        }

        public static RowMeta from(DataInput dis) throws IOException {
            int mapSize = dis.readInt();
            if (mapSize == 0)
                return null;
            RowMeta meta = new RowMeta(mapSize);
            for (int i = 0; i < mapSize; i++) {
                String key = dis.readUTF();
                String value = dis.readUTF();
                meta.put(key, value);
            }
            if (logger.isDebugEnabled())
                logger.debug("Row meta deserializing - " + meta);
            return meta;

        }

        @Override
        public RowMeta deserialize(DataInput dis, int version) throws IOException {
            return from(dis);
        }

        @Override
        public long serializedSize(RowMeta rowMeta, int version) {
            if (rowMeta == null) {
                if (logger.isDebugEnabled())
                    logger.debug("Row meta serializedSize - " + 0);
                return TypeSizes.NATIVE.sizeof(0);
            } else {
                int mapSize = rowMeta.size();
                long size = TypeSizes.NATIVE.sizeof(mapSize);
                for (Map.Entry<String, String> entry : rowMeta.entrySet()) {
                    size = size + TypeSizes.NATIVE.sizeof(entry.getKey());
                    size = size + TypeSizes.NATIVE.sizeof(entry.getValue());
                }
                if (logger.isDebugEnabled())
                    logger.debug("Row meta serializedSize - " + size);
                return size;
            }
        }
    }

    public static class RowMetaComparator extends AbstractType<Map<String, String>> {
        private final CQL3Type.Native[] kinds;
        private final String[] orders;

        protected static MapType<String, String> mapType = MapType.getInstance(UTF8Type.instance, UTF8Type.instance);

        public static RowMetaComparator getInstance(RowMeta map) {
            String orderColsStr = map.get(RowMeta.ORDER_COLUMNS);
            String orderTypesStr = map.get(RowMeta.ORDER_TYPES);
            String[] orderCols = StringUtils.split(orderColsStr, ',');
            String[] orderTypes = StringUtils.split(orderTypesStr, ',');
            if (orderCols == null || orderTypes == null) {
                return null;
            } else {
                return new RowMetaComparator(orderCols, orderTypes);
            }
        }

        public static RowMetaComparator getSerDeInstance() {
            return new RowMetaComparator();
        }

        /**
         * Dont use anywhere else.
         */
        private RowMetaComparator() {
            this.orders = null;
            this.kinds = null;
        }


        private RowMetaComparator(String[] orders, String[] kindStrings) {
            this.orders = orders;
            this.kinds = new CQL3Type.Native[kindStrings.length];
            for (int i = 0; i < kindStrings.length; i++) {
                this.kinds[i] = CQL3Type.Native.valueOf(kindStrings[i]);
            }
        }

        public int compare(ByteBuffer a, ByteBuffer b) {
            Map<String, String> aMap = compose(a);
            Map<String, String> bMap = compose(b);
            for (int i = 0; i < orders.length; i++) {
                String column = orders[i];
                CQL3Type.Native kind = kinds[i];
                AbstractType type = kind.getType();
                String aValue = aMap.get(column);
                String bValue = bMap.get(column);
                int comparison = type.compare(type.fromString(aValue), type.fromString(bValue));
                if (comparison != 0)
                    return comparison;
            }

            return 0;
        }

        @Override
        public Map<String, String> compose(ByteBuffer bytes) {
            String json = UTF8Type.instance.compose(bytes);
            return RowMeta.fromJson(json);
        }

        @Override
        public ByteBuffer decompose(Map<String, String> value) {
            RowMeta rowMeta = new RowMeta(value);
            return UTF8Type.instance.decompose(rowMeta.toJson(value));
        }

        @Override
        public String getString(ByteBuffer bytes) {
            return UTF8Type.instance.getString(bytes);
        }

        @Override
        public ByteBuffer fromString(String source) throws MarshalException {
            return UTF8Type.instance.fromString(source);
        }

        @Override
        public void validate(ByteBuffer bytes) throws MarshalException {
            UTF8Type.instance.validate(bytes);
        }
    }

}
