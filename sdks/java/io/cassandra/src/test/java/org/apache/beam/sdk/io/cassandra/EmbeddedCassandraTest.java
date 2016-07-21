package org.apache.beam.sdk.io.cassandra;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * A simple unit test that just ensures that the cassandra embedded database is running.
 * <p>
 * It is important that the embedded database is compatible with the spark connector.
 */
public class EmbeddedCassandraTest {

    @Rule
    public EmbeddedCassandraResource mCass = new EmbeddedCassandraResource(getClass()
            .getSimpleName());

    /**
     * Tests the set-up of the embedded Cassandra server. This can be complicated since the
     * embedded server needs to
     * match the versions of cassandra used within the spark connector.
     */
    @Test
    public void testEmbeddedCassandra() {
        // Setup.
        mCass.execute("CREATE TABLE example (name text PRIMARY KEY)");
        mCass.execute("INSERT INTO example (name) values ('hello')");
        mCass.execute("INSERT INTO example (name) values ('world')");

        // Query.
        ResultSet rs = mCass.execute("SELECT name FROM example");
        List<String> result = new ArrayList<>();
        for (Row r : rs) {
            String value = r.getString(0);
            result.add(value);
        }

        // Validate.
        assertThat(result, hasSize(2));
        assertThat(result, containsInAnyOrder("hello", "world"));
    }
}
