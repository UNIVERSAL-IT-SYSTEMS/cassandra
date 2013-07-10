package org.apache.cassandra.db;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.IVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * User: satya
 * <p/>
 * A Map to contain row meta data.
 */
public class RowMeta extends HashMap<String, String> {
    protected static final Logger logger = LoggerFactory.getLogger(RowMeta.class);

    public static final ColumnIdentifier ROW_META_COLUMN = new ColumnIdentifier("_row_meta_", false);
    public static final UTF8Type ROW_META_TYPE = UTF8Type.instance;
    public static final String ROW_META_COMPRATOR_CLASS = "ROW_META_COMPRATOR_CLASS";
    public static final String DEFAULT_ROW_META_COMPRATOR_CLASS = "DEFAULT_ROW_META_COMPRATOR_CLASS";

    public static final RowMetaSerializer serializer = new RowMetaSerializer();

    public AbstractType rowMetaComparator() {
        if (size() > 0) {
            String compClass = get(ROW_META_COMPRATOR_CLASS);
            if (DEFAULT_ROW_META_COMPRATOR_CLASS.equals(compClass)) {
                return null;
            } else {
                try {
                    AbstractType type = (AbstractType) Class.forName(compClass).newInstance();
                    return type;
                } catch (Throwable e) {
                    logger.debug("Row Meta comparator class not foun - Returning null", e);
                    return null;
                }
            }
        }
        return null;
    }

    public RowMeta(int size) {
        super(size);
    }

    public RowMeta(Map<String, String> map) {
        super(map);
    }

    public String asJson() {
        String json = "{ ";
        for (Map.Entry<String, String> entry : entrySet()) {
            json = json + "'" + entry.getKey() + "':'" + entry.getValue() + "',";
        }
        json.substring(0, json.length() - 1);
        json = json + " }";
        return json;
    }


    public static class RowMetaSerializer implements IVersionedSerializer<RowMeta> {

        @Override
        public void serialize(RowMeta rowMeta, DataOutput dos, int version) throws IOException {
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

        @Override
        public RowMeta deserialize(DataInput dis, int version) throws IOException {
            int mapSize = dis.readInt();
            RowMeta meta = new RowMeta(mapSize);
            for (int i = 0; i < mapSize; i++) {
                String key = dis.readUTF();
                String value = dis.readUTF();
                meta.put(key, value);
            }
            return meta;

        }

        @Override
        public long serializedSize(RowMeta rowMeta, int version) {
            if (rowMeta == null) {
                return TypeSizes.NATIVE.sizeof(0);
            } else {
                int mapSize = rowMeta.size();
                long size = TypeSizes.NATIVE.sizeof(mapSize);
                for (Map.Entry<String, String> entry : rowMeta.entrySet()) {
                    size = size + TypeSizes.NATIVE.sizeof(entry.getKey());
                    size = size + TypeSizes.NATIVE.sizeof(entry.getValue());
                }
                return size;
            }
        }
    }


}
