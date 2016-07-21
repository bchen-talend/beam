package org.apache.beam.sdk.io.cassandra;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class CassandraSourceSinkTest {

    public static final String KS_NAME = CassandraSourceSinkTest.class.getSimpleName()
            .toLowerCase();
    // start the embedded cassandra server and init with data

    public static final String TABLE_SOURCE = "t_src";
    public static final String TABLE_COUNT = "t_count";
    public static final String TABLE_DEST = "t_dest";

    @Rule
    public EmbeddedCassandraExampleDataResource mCass = new EmbeddedCassandraExampleDataResource
            (KS_NAME,
                    TABLE_SOURCE, TABLE_COUNT, TABLE_DEST);

    @Test
    public void readAndWrite() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(CassandraConfig.PORT, EmbeddedCassandraResource.PORT);

        List<CassandraRow> rows = retrieveRows(EmbeddedCassandraResource.HOST, KS_NAME,
                TABLE_SOURCE, config);
        assertEquals(6, rows.size());

        CassandraIO.CassandraSink sink = new CassandraIO.CassandraSink(EmbeddedCassandraResource
                .HOST, KS_NAME,
                TABLE_DEST, config, null);
        CassandraIO.CassandraWriteOperation writeOperation =
                (CassandraIO.CassandraWriteOperation) sink.createWriteOperation(null);
        CassandraIO.CassandraWriter writer = (CassandraIO.CassandraWriter) writeOperation
                .createWriter(null);
        writer.open(null);
        for (CassandraRow row : rows) {
            writer.write(row);
        }
        writer.close();

        rows = retrieveRows(EmbeddedCassandraResource.HOST, KS_NAME, TABLE_DEST, config);
        assertEquals(6, rows.size());

    }

    private List<CassandraRow> retrieveRows(String host, String keyspace, String table,
                                            Map<String, String> config)
            throws Exception {
        List<CassandraRow> rows = new ArrayList<>();

        CassandraIO.CassandraSource source = new CassandraIO.CassandraSource(host, keyspace,
                table, config, null,
                null, null);
        long estimatedSizeBytes = source.getEstimatedSizeBytes(null);
        List<CassandraIO.CassandraSource> cassandraSources = source.splitIntoBundles
                (estimatedSizeBytes, null);
        for (CassandraIO.CassandraSource cassandraSource : cassandraSources) {
            CassandraIO.CassandraReader reader = (CassandraIO.CassandraReader) cassandraSource
                    .createReader(null);
            try {
                for (boolean available = reader.start(); available; available = reader.advance()) {
                    CassandraRow current = reader.getCurrent();
                    rows.add(current);
                }
            } finally {
                reader.close();
            }
        }
        return rows;
    }


}
