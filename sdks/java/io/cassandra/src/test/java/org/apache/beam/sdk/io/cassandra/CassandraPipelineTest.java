package org.apache.beam.sdk.io.cassandra;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class CassandraPipelineTest {
    public static final String KS_NAME = CassandraPipelineTest.class.getSimpleName()
            .toLowerCase();
    // start the embedded cassandra server and init with data

    public static final String TABLE_SOURCE = "t_src";
    public static final String TABLE_DEST = "t_dest";
    public static final int ROW_COUNT = 100;

    @Rule
    public EmbeddedCassandraSimpleDataResource mCass = new EmbeddedCassandraSimpleDataResource
            (KS_NAME, TABLE_SOURCE, TABLE_DEST, ROW_COUNT);

    @Test
    public void readAndWrite() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(CassandraConfig.PORT, EmbeddedCassandraResource.PORT);

        Pipeline p = TestPipeline.create();

        p
                .apply(CassandraIO.read().withHost(EmbeddedCassandraResource.HOST).withKeyspace
                        (KS_NAME).withTable(TABLE_SOURCE).withConfig(config)).apply(CassandraIO
                .write().withHost(EmbeddedCassandraResource.HOST).withKeyspace(KS_NAME).withTable
                        (TABLE_DEST).withConfig(config));

        p.run();


        List<CassandraRow> rows = retrieveRows(EmbeddedCassandraResource.HOST, KS_NAME,
                TABLE_DEST, config);
        assertEquals(ROW_COUNT, rows.size());

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
