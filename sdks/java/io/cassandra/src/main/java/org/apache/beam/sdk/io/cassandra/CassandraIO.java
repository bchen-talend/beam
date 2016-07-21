package org.apache.beam.sdk.io.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Sink;
import org.apache.beam.sdk.io.cassandra.write.BatchCacheExecutor;
import org.apache.beam.sdk.io.cassandra.write.BatchGroupingKey;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TokenRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.annotation.Nullable;

/**
 *
 */
public class CassandraIO {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraIO.class);

    public static Read read() {
        return new Read(null, null, null, null, null, null);
    }

    public static Write write() {
        return new Write(null, null, null, null, null);
    }

    /**
     *
     */
    public static class Read extends PTransform<PBegin, PCollection<CassandraRow>> {

        protected final String host;
        protected final String keyspace;
        protected final String table;
        protected final Map<String, String> config;
        protected final List<String> selected;
        protected final Map<String, Object> conditions;

        private Read(String host, String keyspace, String table, Map<String, String> config,
                     List<String> selected,
                     Map<String, Object> conditions) {
            this.host = host;
            this.keyspace = keyspace;
            this.table = table;
            this.config = config;
            this.selected = selected;
            this.conditions = conditions;
        }

        public Read withHost(String host) {
            return new Read(host, keyspace, table, config, selected, conditions);
        }

        public Read withKeyspace(String keyspace) {
            return new Read(host, keyspace, table, config, selected, conditions);
        }

        public Read withTable(String table) {
            return new Read(host, keyspace, table, config, selected, conditions);
        }

        public Read withConfig(Map<String, String> config) {
            return new Read(host, keyspace, table, config, selected, conditions);
        }

        public Read select(String... columnNames) {
            return new Read(host, keyspace, table, config, Arrays.asList(columnNames), conditions);
        }

        public Read where(String key, Object value) {
            Map<String, Object> newConditions = new HashMap<>(conditions);
            newConditions.put(key, value);
            return new Read(host, keyspace, table, config, selected, newConditions);
        }

        @Override
        public PCollection<CassandraRow> apply(PBegin input) {
            final org.apache.beam.sdk.io.Read.Bounded<CassandraRow> transform = org.apache.beam
                    .sdk.io.Read.from(new CassandraSource(host, keyspace, table, config,
                            selected, conditions, null));
            return input.getPipeline().apply(transform);
        }
    }

    /**
     *
     */
    protected static class CassandraSource extends BoundedSource<CassandraRow> {

        private String host;
        private String keyspace;
        private String table;
        private Map<String, String> config;
        private List<String> selected;
        private Map<String, Object> conditions;
        private List<TokenRange> tokenRanges;

        public CassandraSource(String host, String keyspace, String table, Map<String, String>
                config, List<String>
                                       selected, Map<String, Object> conditions, List<TokenRange>
                                       tokenRanges) {
            this.host = host;
            this.keyspace = keyspace;
            this.table = table;
            this.config = config;
            this.selected = selected;
            this.conditions = conditions;
            this.tokenRanges = tokenRanges;
        }

        private Cluster getCluster() {
            // make the port has default value
            return Cluster.builder().addContactPointsWithPorts(new InetSocketAddress(host,
                    Integer.valueOf(config.get(CassandraConfig.PORT)))).build();
        }

        private Session getConnection() {
            return getCluster().connect();
        }

        private Metadata getMetadata() {
            return getCluster().getMetadata();
        }

        private List<TokenRange> getTokenRanges() {
            Metadata metadata = getMetadata();
            List<TokenRange> tokenRanges = new ArrayList<TokenRange>();
            for (TokenRange tokenRange : metadata.getTokenRanges()) {
                for (TokenRange unwrapTokenRange : tokenRange.unwrap()) {
                    tokenRanges.add(unwrapTokenRange);
                }
            }
            return tokenRanges;
        }

        private List<List<TokenRange>> getSplitGroup(int maxSize) {
            List<TokenRange> tokenRanges = getTokenRanges();
            int tokenRangesSize = tokenRanges.size();
            List<List<TokenRange>> groups = new ArrayList<List<TokenRange>>();
            int groupCount = (tokenRangesSize / maxSize) + (tokenRangesSize % maxSize > 0 ? 1 : 0);
            for (int i = 0; i < groupCount; i++) {
                groups.add(new ArrayList<TokenRange>(tokenRanges.subList(i * maxSize, Math.min
                        (maxSize,
                                tokenRangesSize))));
            }
            return groups;
        }

        //desiredBundleSizeBytes is the size of tokenRange list here
        @Override
        public List<CassandraSource> splitIntoBundles(long desiredBundleSizeBytes,
                                                      PipelineOptions options) throws
                Exception {
            int trGroupMaxSize = Long.valueOf(desiredBundleSizeBytes).intValue();
            List<List<TokenRange>> splitGroup = getSplitGroup(trGroupMaxSize);

            List<CassandraSource> result = new ArrayList<CassandraSource>();
            if (splitGroup.size() == 1) {
                result.add(new CassandraSource(host, keyspace, table, config, selected,
                        conditions, null));
            } else {
                for (int i = 0; i < splitGroup.size(); i++) {
                    List<TokenRange> tokenRanges = splitGroup.get(i);

                    LOG.info("Partitions assigned to split {} (total {}): {}",
                            i, tokenRanges.size(), Joiner.on(",").join(tokenRanges));

                    result.add(new CassandraSource(host, keyspace, table, config, selected,
                            conditions, tokenRanges));
                }
            }
            return result;
        }

        /**
         * TODO is there any better way to calculate partition?
         * use system.size_estimates is not really right, try with rangeBySystemTable in
         * CassandraIOTest
         */
        @Override
        public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
            return getTokenRanges().size();
        }

        @Override
        public boolean producesSortedKeys(PipelineOptions options) throws Exception {
            return false;
        }

        private Set<String> getParitionKey() {
            Set<String> result = new HashSet<>();
            KeyspaceMetadata ksMetadata = getMetadata().getKeyspace(keyspace);
            if (ksMetadata != null) {
                AbstractTableMetadata tbMetadata = ksMetadata.getTable(table);
                if (tbMetadata == null) {
                    tbMetadata = ksMetadata.getMaterializedView(table);
                }
                if (tbMetadata != null) {
                    List<ColumnMetadata> pkeys = tbMetadata.getPartitionKey();
                    for (ColumnMetadata pkey : pkeys) {
                        result.add(pkey.getName());
                    }
                }
            }
            return result;
        }

        private String getPartitionKeyClause() {
            StringBuilder sb = new StringBuilder();
            boolean needComma = false;
            for (String s : getParitionKey()) {
                if (needComma) {
                    sb.append(", ");
                }
                sb.append(Metadata.quote(s));
                needComma = true;
            }
            return sb.toString();
        }

        //TODO add conditions
        private String genCqlByTokenRange(TokenRange tokenRange) {
            String columns = "*";
            if (selected != null && selected.size() > 0) {
                columns = Joiner.on(',').join(selected);
            }
            String baseCql = "select " + columns + " from " + Metadata.quote(keyspace) + "."
                    + Metadata.quote(table);
            if (tokenRange == null) {
                return baseCql;
            } else {
                return baseCql + " where token(" + getPartitionKeyClause() + ") > " + tokenRange
                        .getStart()
                        + " AND token(" + getPartitionKeyClause() + ") <= " + tokenRange.getEnd();
            }
        }

        @Override
        public BoundedReader createReader(PipelineOptions options) throws IOException {
            List<String> cqls = new ArrayList<>();
            if (tokenRanges == null) {
                cqls.add(genCqlByTokenRange(null));
                return new CassandraReader(this, cqls);
            }
            for (TokenRange tokenRange : tokenRanges) {
                cqls.add(genCqlByTokenRange(tokenRange));
            }
            return new CassandraReader(this, cqls);
        }

        @Override
        public void validate() {
            checkNotNull(host, "Cassandra host should be set");
            checkNotNull(keyspace, "Cassandra keyspace should be set");
            checkNotNull(table, "Cassandra table should be set");
        }

        public Coder getDefaultOutputCoder() {
            return SerializableCoder.of(CassandraRow.class);
        }
    }

    /**
     *
     */
    protected static class CassandraReader extends BoundedSource.BoundedReader<CassandraRow> {

        private final CassandraSource source;
        private Iterator<String> cqls;
        private ResultSet rs;
        private com.datastax.driver.core.Row current;

        public CassandraReader(CassandraSource source, List<String> cqls) {
            this.source = source;
            this.cqls = cqls.iterator();
        }

        @Override
        public boolean start() throws IOException {
            fetchWithNextTokenRange();
            current = rs.one();
            return current != null;
        }

        private void fetchWithNextTokenRange() {
            if (cqls.hasNext()) {
                String cql = cqls.next();
                Session connect = source.getConnection();
                rs = connect.execute(cql);
            }
        }

        @Override
        public boolean advance() throws IOException {
            current = rs.one();
            if (current == null) {
                fetchWithNextTokenRange();
                current = rs.one();
            }
            return current != null;
        }

        @Override
        public CassandraRow getCurrent() throws NoSuchElementException {
            return CassandraRow.fromJavaDriverRow(current);
        }

        @Override
        public void close() throws IOException {

        }

        public BoundedSource getCurrentSource() {
            return source;
        }
    }

    /**
     *
     */
    public static class Write extends PTransform<PCollection<CassandraRow>, PDone> {

        protected final String host;
        protected final String keyspace;
        protected final String table;
        protected final Map<String, String> config;
        protected final List<String> columns;

        private Write(String host, String keyspace, String table, Map<String, String> config,
                      List<String> columns) {
            this.host = host;
            this.keyspace = keyspace;
            this.table = table;
            this.config = config;
            this.columns = columns;
        }

        public Write withHost(String host) {
            return new Write(host, keyspace, table, config, columns);
        }

        public Write withKeyspace(String keyspace) {
            return new Write(host, keyspace, table, config, columns);
        }

        public Write withTable(String table) {
            return new Write(host, keyspace, table, config, columns);
        }

        public Write withConfig(Map<String, String> config) {
            return new Write(host, keyspace, table, config, columns);
        }

        public Write withColumns(String... columns) {
            return new Write(host, keyspace, table, config, Arrays.asList(columns));
        }

        @Override
        public PDone apply(PCollection<CassandraRow> input) {
            return input.apply(org.apache.beam.sdk.io.Write.to(new CassandraSink(host, keyspace,
                    table, config,
                    columns)));
        }
    }

    /**
     *
     */
    protected static class CassandraSink extends Sink<CassandraRow> {

        private String host;
        private String keyspace;
        private String table;
        private Map<String, String> config;
        private List<String> columns;

        public CassandraSink(String host, String keyspace, String table, Map<String, String>
                config, List<String>
                                     columns) {
            this.host = host;
            this.keyspace = keyspace;
            this.table = table;
            this.config = config;
            this.columns = columns;
        }

        @Override
        public void validate(PipelineOptions options) {

        }

        @Override
        public WriteOperation<CassandraRow, ?> createWriteOperation(PipelineOptions options) {
            return new CassandraWriteOperation(this);
        }

        private Cluster getCluster() {
            //make the port has default value
            return Cluster.builder().addContactPointsWithPorts(new InetSocketAddress(host,
                    Integer.valueOf(config.get
                            (CassandraConfig.PORT)))).build();

        }

        private Session getConnection() {
            return getCluster().connect();
        }

        private Metadata getMetadata() {
            return getCluster().getMetadata();
        }

        private List<String> getColumns() {
            if (columns != null && columns.size() > 0) {
                return columns;
            }
            List<String> result = new ArrayList<>();
            KeyspaceMetadata ksMetadata = getMetadata().getKeyspace(keyspace);
            if (ksMetadata != null) {
                AbstractTableMetadata tbMetadata = ksMetadata.getTable(table);
                if (tbMetadata == null) {
                    tbMetadata = ksMetadata.getMaterializedView(table);
                }
                if (tbMetadata != null) {
                    List<ColumnMetadata> pkeys = tbMetadata.getColumns();
                    for (ColumnMetadata pkey : pkeys) {
                        result.add(pkey.getName());
                    }
                }
            }
            return result;
        }
    }

    /**
     *
     */
    protected static class CassandraWriteOperation extends Sink.WriteOperation<CassandraRow, Long> {

        private CassandraSink cassandraSink;

        public CassandraWriteOperation(CassandraSink cassandraSink) {
            this.cassandraSink = cassandraSink;
        }

        @Override
        public void initialize(PipelineOptions options) throws Exception {

        }

        @Override
        public void finalize(Iterable<Long> writerResults, PipelineOptions options) throws
                Exception {

        }

        @Override
        public Sink.Writer<CassandraRow, Long> createWriter(PipelineOptions options) throws
                Exception {
            return new CassandraWriter(this);
        }

        @Override
        public Sink<CassandraRow> getSink() {
            return cassandraSink;
        }

        @Override
        public Coder<Long> getWriterResultCoder() {
            return VarLongCoder.of();
        }
    }

    /**
     *
     */
    protected static class CassandraWriter extends Sink.Writer<CassandraRow, Long> {
        private final CassandraWriteOperation writeOperation;
        private final CassandraSink sink;
        private BatchCacheExecutor executor;
        private PreparedStatement prepareStmt;
        private long recordsWritten;

        public CassandraWriter(CassandraWriteOperation writeOperation) {
            this.writeOperation = writeOperation;
            this.sink = (CassandraSink) writeOperation.getSink();
        }

        @Override
        public void open(String uId) throws Exception {
            //TODO all the parameter should be configurable in sink.config
            executor = new BatchCacheExecutor(sink.getConnection(), sink.keyspace, sink.table,
                    sink.getColumns(),
                    BatchGroupingKey.Replica, 1000, true, 5);
            //TODO support update statement
            prepareStmt = sink.getConnection().prepare(genStmt());
            recordsWritten = 0;
        }

        private String genStmt() {
            return "insert into " + Metadata.quote(sink.keyspace) + "." + Metadata.quote(sink.table)
                    + " (" + Joiner.on(',').join(sink.getColumns()) + " ) values (" + Joiner.on
                    (',').join
                    (Collections2.transform(sink.getColumns(), new Function<String, String>() {
                        @Nullable
                        @Override
                        public String apply(@Nullable String s) {
                            return "?";
                        }
                    })) + ")";
        }

        @Override
        public void write(CassandraRow value) throws Exception {
            BoundStatement boundStatement = new BoundStatement(prepareStmt);
            boundStatement.bind(value.getValues().toArray());
            executor.addOrExecBatch(boundStatement);
            ++recordsWritten;
        }

        @Override
        public Long close() throws Exception {
            //can we do endBatch here?
            executor.endBatch();
            return recordsWritten;
        }

        @Override
        public Sink.WriteOperation<CassandraRow, Long> getWriteOperation() {
            return writeOperation;
        }
    }

}
