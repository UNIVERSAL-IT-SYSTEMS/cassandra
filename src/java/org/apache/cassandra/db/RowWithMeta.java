package org.apache.cassandra.db;

import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: satya
 */
public class RowWithMeta extends Row {
    public RowMeta rowMeta;

    public RowWithMeta(DecoratedKey key, ColumnFamily cf, RowMeta rowMeta) {
        super(key, cf);
        this.rowMeta = rowMeta;
    }

    public static class RowWithMetaSerializer extends RowSerializer {
        public void serialize(Row row, DataOutput dos, int version) throws IOException {
            ByteBufferUtil.writeWithShortLength(row.key.key, dos);
            ColumnFamily.serializer.serialize(row.cf, dos, version);
            if (row instanceof RowWithMeta) {
                RowMeta.serializer.serialize(((RowWithMeta) row).rowMeta, dos, version);
            } else {
                RowMeta.serializer.serialize(null, dos, version);
            }
        }

        public Row deserialize(DataInput dis, int version, IColumnSerializer.Flag flag, ISortedColumns.Factory factory) throws IOException {
            return new RowWithMeta(StorageService.getPartitioner().decorateKey(ByteBufferUtil.readWithShortLength(dis)),
                    ColumnFamily.serializer.deserialize(dis, flag, factory, version), RowMeta.serializer.deserialize(dis, version));
        }

        public long serializedSize(Row row, int version) {
            int keySize = row.key.key.remaining();
            long rowMetaSize;
            if (row instanceof RowWithMeta) {
                rowMetaSize = RowMeta.serializer.serializedSize(((RowWithMeta) row).rowMeta, version);
            } else {
                rowMetaSize = RowMeta.serializer.serializedSize(null, version);
            }
            return TypeSizes.NATIVE.sizeof((short) keySize) + keySize + ColumnFamily.serializer.serializedSize(row.cf, TypeSizes.NATIVE, version) + rowMetaSize;
        }
    }
}
