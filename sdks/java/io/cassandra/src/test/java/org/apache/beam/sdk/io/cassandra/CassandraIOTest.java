package org.apache.beam.sdk.io.cassandra;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.io.BoundedSource;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TokenRange;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;


/**
 *
 */
public class CassandraIOTest {

    private static String h1 = "10.2.1.1";

    //create KEYSPACE myks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} ;
    //create TABLE myks.mytable ( id int, name text, age int, primary KEY (id, name));
    private static String h2 = "10.2.1.2";

    public static void prepare(int replic, int nbline) {
        Cluster cluster = getCluster();
        Session connect = cluster.connect();
        connect.execute("drop keyspace if exists myks");
        connect.execute("create KEYSPACE myks WITH replication = {'class': 'SimpleStrategy', "
                + "'replication_factor': "
                + replic + "}");
        connect.execute("create TABLE myks.mytable ( id text, name text, age int, primary KEY "
                + "(id, name))");
        PreparedStatement prepare = connect.prepare("insert into myks.mytable(id, name, age) "
                + "values (?, ?, ?)");
        Random random = new Random();
        String[] ids = new String[]{"aaa", "bbb", "ccc"};
        for (int i = 0; i < nbline; i++) {
            BoundStatement bound = prepare.bind(ids[random.nextInt(3)], "n" + i, 10 + i);
            connect.executeAsync(bound);
        }
        connect.close();
    }

    private static Cluster getCluster() {
        return Cluster.builder()
                .addContactPoint("localhost")
//                .addContactPoint(h1)
//                .addContactPoint(h2)
                .build();
    }

    @Test
    @Ignore
    public void rowSerialable() throws Exception {
        prepare(1, 1);
        CassandraIO.CassandraSource source = new CassandraIO.CassandraSource("localhost", "myks",
                "mytable", null,
                null, null, null);
        List<? extends BoundedSource> boundedSources = source.splitIntoBundles(source
                        .getEstimatedSizeBytes(null),
                null);
        assertEquals(1, boundedSources.size());
        for (BoundedSource boundedSource : boundedSources) {
            CassandraIO.CassandraReader reader = (CassandraIO.CassandraReader) boundedSource
                    .createReader(null);
            try {
                for (boolean available = reader.start(); available; available = reader.advance()) {
                    CassandraRow current = reader.getCurrent();
                    Serializable original = current;
                    Serializable copy = SerializationUtils.clone(original);
                    System.out.print(copy);
                }
            } finally {
                reader.close();
            }
        }

    }

    @Test
    @Ignore
    public void tokenRangeSize() {
        Map<String, String> tokenRangeFromSizeEstimates = getTokenRangeFromSizeEstimates();
        assertEquals(256, tokenRangeFromSizeEstimates.size());
    }

    private Map<String, String> getTokenRangeFromSizeEstimates() {
        Cluster cluster = getCluster();
        Session connect = cluster.connect();
        ResultSet rs = connect.execute("SELECT range_start, range_end, partitions_count, "
                + "mean_partition_size "
                + "FROM system.size_estimates "
                + "WHERE keyspace_name = ? AND table_name = ?", "myks", "mytable");
        Map<String, String> tokenRanges = new HashMap<String, String>();
        List<com.datastax.driver.core.Row> all = rs.all();
        System.out.println("counts from system table" + all.size());
        for (com.datastax.driver.core.Row row : all) {
            tokenRanges.put(row.getString("range_start"), row.getString("range_end"));
            if (row.getLong("mean_partition_size") > 0) {
                System.out.println("find: " + row.getString("range_start") + " - " + row
                        .getString("range_end") + " -" + " " + row.getLong("partitions_count")
                        + " - " + row.getLong
                        ("mean_partition_size"));
            }
        }
        return tokenRanges;
    }

    @Test
    @Ignore
    public void partition() {
        Cluster cluster = getCluster();
        Metadata metadata = cluster.getMetadata();
        Session connect = cluster.connect();
        Set<Host> allHosts = metadata.getAllHosts();
        System.out.println(allHosts);
        int total = 0;
        Set<String> ranges = new HashSet<String>();
        int rangesize = 0;
        int emptyRange = 0;
        Set<TokenRange> tokenRanges = metadata.getTokenRanges();
//            Set<TokenRange> tokenRanges = metadata.getTokenRanges("myks", host);
        System.out.println(tokenRanges);
        System.out.println(tokenRanges.size());
        for (TokenRange tokenRange : tokenRanges) {
            for (TokenRange range : tokenRange.unwrap()) {
//                Set<Host> tr_hosts = metadata.getReplicas("myks", tokenRange);
                rangesize += 1;
                ranges.add(range.getStart().getValue() + "" + range.getEnd().getValue());
                ResultSet rs = connect.execute("select * from myks.mytable where token(id) > "
                        + range.getStart() + ""
                        + " AND token(id) <= " + range.getEnd());
                List<com.datastax.driver.core.Row> all = rs.all();
                total += all.size();
                if (all.size() == 0) {
                    emptyRange += 1;
                } else {
                    System.out.println("non empty range: " + range.getStart() + " - " + range
                            .getEnd());
                }
            }
        }
//            System.out.println("Host:" + host + " ; rows:" + rangesize);
        System.out.println("range to row count:" + total);
        System.out.println("range count:" + rangesize);
        System.out.println("ranges: " + ranges.size());
        System.out.println("empty ranges: " + emptyRange);

    }

    @Test
    @Ignore
    public void replica() {
        Cluster cluster = getCluster();
        Metadata metadata = cluster.getMetadata();
        Session connect = cluster.connect();
        Set<Host> allHosts = metadata.getAllHosts();
        System.out.println(allHosts);
        int total = 0;
        Set<String> ranges = new HashSet<String>();
        int rangesize = 0;
        int emptyRange = 0;
//        Set<TokenRange> tokenRanges = metadata.getTokenRanges();
        Host[] hosts = allHosts.toArray(new Host[allHosts.size()]);
        Host host = null;
        for (Host h : hosts) {
            if (h1.equals(h.getAddress().getHostAddress())) {
                host = h;
            }
        }

        Set<TokenRange> tokenRanges = metadata.getTokenRanges("myks", host);
        System.out.println(tokenRanges);
        System.out.println(tokenRanges.size());
        for (TokenRange tokenRange : tokenRanges) {
            for (TokenRange range : tokenRange.unwrap()) {
//                Set<Host> tr_hosts = metadata.getReplicas("myks", tokenRange);
                rangesize += 1;
                ranges.add(range.getStart().getValue() + "" + range.getEnd().getValue());
                ResultSet rs = connect.execute("select * from myks.mytable where token(id) > "
                        + range.getStart() + ""
                        + " AND token(id) <= " + range.getEnd());
                List<com.datastax.driver.core.Row> all = rs.all();
                total += all.size();
                if (all.size() == 0) {
                    emptyRange += 1;
                } else {
                    System.out.println("non empty range: " + range.getStart() + " - " + range
                            .getEnd());
                }
            }
        }
//            System.out.println("Host:" + host + " ; rows:" + rangesize);
        System.out.println("range to row count:" + total);
        System.out.println("range count:" + rangesize);
        System.out.println("ranges: " + ranges.size());
        System.out.println("empty ranges: " + emptyRange);

    }

    @Test
    @Ignore
    public void rangeBySystemTable() {
        Cluster cluster = getCluster();
        Metadata metadata = cluster.getMetadata();
        Session connect = cluster.connect();
        Set<Host> allHosts = metadata.getAllHosts();
        System.out.println(allHosts);
        int total = 0;
        Set<String> ranges = new HashSet<String>();
        int rangesize = 0;
        int emptyRange = 0;
        Set<TokenRange> tokenRanges = metadata.getTokenRanges();
//            Set<TokenRange> tokenRanges = metadata.getTokenRanges("myks", host);
        System.out.println(tokenRanges);
        System.out.println(tokenRanges.size());
        Map<String, String> tokenRangeFromSizeEstimates = getTokenRangeFromSizeEstimates();
        for (String key : tokenRangeFromSizeEstimates.keySet()) {

//                Set<Host> tr_hosts = metadata.getReplicas("myks", tokenRange);
            rangesize += 1;
            ranges.add(key + "" + tokenRangeFromSizeEstimates.get(key));
            ResultSet rs = connect.execute("select * from myks.mytable where token(id) > " + key
                    + " AND token(id) <="
                    + " " + tokenRangeFromSizeEstimates.get(key));
            List<com.datastax.driver.core.Row> all = rs.all();
            total += all.size();
            if (all.size() == 0) {
                emptyRange += 1;
            } else {
                System.out.println("non empty range: " + key + " - "
                        + tokenRangeFromSizeEstimates.get(key));
            }
        }

//            System.out.println("Host:" + host + " ; rows:" + rangesize);
        System.out.println("range to row count:" + total);
        System.out.println("range count:" + rangesize);
        System.out.println("ranges: " + ranges.size());
        System.out.println("empty ranges: " + emptyRange);

    }
}
