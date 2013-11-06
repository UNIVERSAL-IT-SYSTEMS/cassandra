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
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * User: satya
 */
public class ExtendedDelegatingFilter implements IDiskAtomFilter {

    protected static final Logger logger = LoggerFactory.getLogger(ExtendedDelegatingFilter.class);

    public static final IVersionedSerializer<ExtendedDelegatingFilter> serializer;
    public static String SERDE_CLASS = "com.tuplejump.stargate.cas.engine.EDFSerializer";

    public static Method method;

    IDiskAtomFilter delegate;
    Map<String, List<RawSelector>> additionalClauses;
    List<Boolean> orderings;
    int limit;
    int skip;

    static {
        try {
            Class<?> clazz = Class.forName(SERDE_CLASS);
            Object instance = clazz.newInstance();
            serializer = (IVersionedSerializer<ExtendedDelegatingFilter>) instance;
            method = instance.getClass().getMethod("deserialize", DataInput.class, int.class, AbstractType.class);
            logger.warn("Loaded Serializer class {}", SERDE_CLASS);
        } catch (Exception e) {
            logger.error("Failed loading Serializer class ", e);
            throw new RuntimeException(e);
        }
    }

    public static ExtendedDelegatingFilter deserialize(DataInput dis, int version, AbstractType comparator) throws IOException {
        try {
            return (ExtendedDelegatingFilter) method.invoke(serializer, dis, version, comparator);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public IDiskAtomFilter getDelegate() {
        return delegate;
    }

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

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder("Additional clauses -\n");
        s.append(additionalClauses).append("\n").append("Orderings -").append(orderings)
                .append("Limit -").append(limit).append("Skip -").append(skip);
        return s.toString();
    }
}
