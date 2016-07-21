package org.apache.beam.sdk.io.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Provides an Cassandra database for JUnit tests. Before each unit test, a keyspace named after
 * the test class name is
 * created and USEd.
 */
public class EmbeddedCassandraResource extends ExternalResource {

    /**
     * Cassandra default YAML (fetched as a resource, so relative to classpath).
     */
    public static final String DEFAULT_CASSANDRA_YAML = "cassandra.yml";

    /**
     * Cassandra host.
     */
    public static final String HOST = "localhost";

    /**
     * Cassandra port. Must match cassandra.yml in src/test/resources/
     */
    public static final String PORT = "59042";
    private final String mKeySpace;
    private Session mSession;
    /**
     * The test name.
     */
    private String mName;

    public EmbeddedCassandraResource(String keySpace, String cassandraYaml, String tmpDir, long
            startupTimeout) {
        mKeySpace = keySpace.startsWith("\"") ? keySpace : keySpace.toLowerCase();
        try {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra(cassandraYaml, tmpDir,
                    startupTimeout);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public EmbeddedCassandraResource(String keySpace) {
        this(keySpace, DEFAULT_CASSANDRA_YAML,
                // The temporary directory must be on a filesystem that supports links.
                "/tmp/cassandra/" + keySpace + "/" + EmbeddedCassandraServerHelper.DEFAULT_TMP_DIR,
                EmbeddedCassandraServerHelper.DEFAULT_STARTUP_TIMEOUT);
    }


    /**
     * @return A connection to the embedded Cassandra server.
     */
    public Session getSession() {
        return mSession;
    }

    /**
     * @return A short-cut to getSession().execute(query).
     */
    public ResultSet execute(String query) {
        return mSession.execute(query);
    }

    /**
     * @return The keyspace used in this test (usually the same as the test name).
     */
    public String getKeySpace() {
        return mKeySpace;
    }

    /**
     * @return An input test table name in the form testMethodName + "Src".
     */
    public String getName() {
        return mName;
    }

    @Override
    public Statement apply(Statement base, Description d) {
        mName = d.getMethodName();
        return super.apply(base, d);
    }

    @Override
    protected void before() throws Throwable {
        Cluster cluster = new Cluster.Builder().addContactPoints(HOST).withPort(Integer.valueOf
                (PORT))
                .build();
        mSession = cluster.connect();

        // Create a table in the keyspace.
        mSession.execute("CREATE KEYSPACE " + getKeySpace()
                + " WITH replication={'class' : 'SimpleStrategy', 'replication_factor':1}");
        mSession.execute("USE " + getKeySpace());
    }

    ;

    @Override
    protected void after() {
        mSession.execute("DROP KEYSPACE \"" + getKeySpace() + "\"");
        mSession.close();
        super.after();
    }

    ;

}
