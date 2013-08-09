package org.apache.cassandra.db.filter;

import org.apache.cassandra.cql3.statements.RawSelector;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.ISSTableColumnIterator;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * User: satya
 */
public class ExtendedDelegatingFilter implements IDiskAtomFilter {

    protected static final Logger logger = LoggerFactory.getLogger(ExtendedDelegatingFilter.class);

    IDiskAtomFilter delegate;
    Map<String, List<RawSelector>> additionalClauses;
    List<Boolean> orderings;
    int limit;
    int skip;

    public List<RawSelector> getClause(String key) {
        return additionalClauses.get(key);
    }

    public Map<String, List<RawSelector>> getClauses() {
        return additionalClauses;
    }

    public List<Boolean> getOrderings() {
        return orderings;
    }

    public int getLimit() {
        return limit;
    }

    public int getSkip() {
        return skip;
    }

    public ExtendedDelegatingFilter(IDiskAtomFilter delegate, Map<String, List<RawSelector>> additionalClauses, List<Boolean> orderings, int limit, int skip) {
        this.delegate = delegate;
        this.additionalClauses = additionalClauses;
        this.orderings = orderings;
        this.limit = limit;
        this.skip = skip;
    }


    @Override
    public OnDiskAtomIterator getMemtableColumnIterator(ColumnFamily cf, DecoratedKey key) {
        return delegate.getMemtableColumnIterator(cf, key);
    }

    @Override
    public ISSTableColumnIterator getSSTableColumnIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry) {
        return delegate.getSSTableColumnIterator(sstable, file, key, indexEntry);
    }

    @Override
    public ISSTableColumnIterator getSSTableColumnIterator(SSTableReader sstable, DecoratedKey key) {
        return delegate.getSSTableColumnIterator(sstable, key);
    }

    @Override
    public void collectReducedColumns(IColumnContainer container, Iterator<IColumn> reducedColumns, int gcBefore) {
        delegate.collectReducedColumns(container, reducedColumns, gcBefore);
    }

    @Override
    public SuperColumn filterSuperColumn(SuperColumn superColumn, int gcBefore) {
        return delegate.filterSuperColumn(superColumn, gcBefore);
    }

    @Override
    public Comparator<IColumn> getColumnComparator(AbstractType<?> comparator) {
        return delegate.getColumnComparator(comparator);
    }

    @Override
    public boolean isReversed() {
        return delegate.isReversed();
    }

    @Override
    public void updateColumnsLimit(int newLimit) {
        delegate.updateColumnsLimit(newLimit);
    }

    @Override
    public int getLiveCount(ColumnFamily cf) {
        return delegate.getLiveCount(cf);
    }

    public static Serializer serializer = new Serializer();

    public static class Serializer implements IVersionedSerializer<ExtendedDelegatingFilter> {

        @Override
        public void serialize(ExtendedDelegatingFilter filter, DataOutput dos, int version) throws IOException {
            if (logger.isDebugEnabled())
                logger.debug("ExtendedDelegatingFilter serializing - " + filter);

            if (filter.additionalClauses == null) {
                dos.writeInt(0);
            } else {
                dos.writeInt(filter.additionalClauses.size());
                for (Map.Entry<String, List<RawSelector>> entry : filter.additionalClauses.entrySet()) {
                    dos.writeUTF(entry.getKey());
                    List<RawSelector> selectors = entry.getValue();
                    dos.writeInt(selectors.size());
                    for (RawSelector selector : selectors) {
                        selector.serialize(selector, dos, version);
                    }
                }
            }

            if (filter.orderings == null) {
                dos.writeInt(0);
            } else {
                dos.writeInt(filter.orderings.size());
                for (Boolean bool : filter.orderings) {
                    dos.writeBoolean(bool);
                }
            }
            if (logger.isDebugEnabled())
                logger.debug("Writing limit / skip -" + filter.limit + "/" + filter.skip);

            dos.writeInt(filter.limit);
            dos.writeInt(filter.skip);
            IDiskAtomFilter.Serializer.instance.serialize(filter.delegate, dos, version);
        }

        @Override
        public ExtendedDelegatingFilter deserialize(DataInput dis, int version) throws IOException {
            return this.deserialize(dis, version);
        }

        public ExtendedDelegatingFilter deserialize(DataInput dis, int version, AbstractType comparator) throws IOException {
            int mapSize = dis.readInt();
            Map<String, List<RawSelector>> additionalClauses = new HashMap<String, List<RawSelector>>(mapSize);
            for (int i = 0; i < mapSize; i++) {
                String key = dis.readUTF();
                int argsSize = dis.readInt();
                List<RawSelector> args = new ArrayList<RawSelector>(argsSize);
                if (argsSize > 0) {
                    for (int j = 0; j < argsSize; j++) {
                        RawSelector selector = RawSelector.serializer.deserialize(dis, version);
                        args.add(selector);
                    }
                }
                additionalClauses.put(key, args);
            }
            if (logger.isDebugEnabled())
                logger.debug("ExtendedDelegatingFilter deserializing - " + additionalClauses);
            int ordSize = dis.readInt();
            List<Boolean> orderings = new ArrayList<Boolean>();
            for (int i = 0; i < ordSize; i++) {
                orderings.add(dis.readBoolean());
            }
            int limit = dis.readInt();
            int skip = dis.readInt();
            if (logger.isDebugEnabled())
                logger.debug("Reading limit / skip -" + limit + "/" + skip);
            IDiskAtomFilter delegate = IDiskAtomFilter.Serializer.instance.deserialize(dis, version, comparator);
            return new ExtendedDelegatingFilter(delegate, additionalClauses, orderings, limit, skip);

        }

        @Override
        public long serializedSize(ExtendedDelegatingFilter filter, int version) {
            long size = 0;
            if (filter.additionalClauses == null) {
                size += TypeSizes.NATIVE.sizeof(0);
            } else {
                size += TypeSizes.NATIVE.sizeof(filter.additionalClauses.size());
                for (Map.Entry<String, List<RawSelector>> entry : filter.additionalClauses.entrySet()) {
                    size += TypeSizes.NATIVE.sizeof(entry.getKey());
                    List<RawSelector> selectors = entry.getValue();
                    size += TypeSizes.NATIVE.sizeof(selectors.size());
                    for (RawSelector selector : selectors) {
                        size += selector.serializedSize(selector, version);
                    }
                }
            }

            if (filter.orderings == null)
                size += TypeSizes.NATIVE.sizeof(0);
            else {
                size += TypeSizes.NATIVE.sizeof(filter.orderings.size());
                for (Boolean bool : filter.orderings) {
                    size += TypeSizes.NATIVE.sizeof(bool);
                }
            }

            size += TypeSizes.NATIVE.sizeof(filter.limit);
            size += TypeSizes.NATIVE.sizeof(filter.skip);
            size += IDiskAtomFilter.Serializer.instance.serializedSize(filter.delegate, version);
            return size;
        }
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder("Additional clauses -\n");
        s.append(additionalClauses).append("\n").append("Orderings -").append(orderings)
                .append("Limit -").append(limit).append("Skip -").append(skip);
        return s.toString();
    }
}
