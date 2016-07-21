package org.apache.beam.sdk.io.cassandra.write;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Session;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class KeyGenerator {

    List<Integer> keyPositions;
    BatchGroupingKey batchGroupingKey;
    Session connection;
    String keyspace;

    public KeyGenerator(Session connection, String keyspace, String table, List<String> columns,
                        BatchGroupingKey
            batchGroupingKey) {
        this.batchGroupingKey = batchGroupingKey;
        if (batchGroupingKey != BatchGroupingKey.None) {
            this.connection = connection;
            this.keyspace = keyspace;
            keyPositions = new ArrayList<Integer>();
            List<ColumnMetadata> partitionKeys = connection.getCluster().getMetadata()
                    .getKeyspace(keyspace).getTable
                    (table).getPartitionKey();
            List<String> partitionColumnKeys = new ArrayList<String>();
            for (ColumnMetadata partitionKey : partitionKeys) {
                partitionColumnKeys.add(partitionKey.getName());
            }
            int position = 0;
            for (String col : columns) {
                if (partitionColumnKeys.contains(col)) {
                    keyPositions.add(position);
                }
                position++;
            }
        }
    }

    public Comparable genKey(BoundStatement boundStatement) throws Exception {
        switch (batchGroupingKey) {
            case Partition:
                return getKey(boundStatement);
            case Replica:
                return connection.getCluster().getMetadata().getReplicas(keyspace, getKey
                        (boundStatement)).hashCode();
            case None:
            default:
                return null;
        }
    }

    private ByteBuffer getKey(BoundStatement stmt) throws Exception {
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        for (int position : keyPositions) {
            if (stmt.isNull(position)) {
                throw new Exception("Cassandra do not allow null value for key");
            }
            keys.add(stmt.getBytesUnsafe(position));
        }
        if (keys.size() == 1) {
            return keys.get(0);
        } else {
            return composeKeys(keys);
        }
    }

    private ByteBuffer composeKeys(List<ByteBuffer> buffers) {
        int totalLength = 0;
        for (ByteBuffer buffer : buffers) {
            totalLength += buffer.remaining() + 3;
        }
        ByteBuffer out = ByteBuffer.allocate(totalLength);
        for (ByteBuffer buffer : buffers) {
            ByteBuffer bb = buffer.duplicate();
            out.put((byte) ((bb.remaining() >> 8) & 0xFF));
            out.put((byte) (bb.remaining() & 0xFF));
            out.put(bb);
            out.put((byte) 0);
        }
        out.flip();
        return out;
    }

}
