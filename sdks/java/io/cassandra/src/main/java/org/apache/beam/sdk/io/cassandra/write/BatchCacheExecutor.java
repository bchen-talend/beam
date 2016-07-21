package org.apache.beam.sdk.io.cassandra.write;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class BatchCacheExecutor {

    final Logger logger = LoggerFactory.getLogger(BatchCacheExecutor.class);

    //TODO make the batchGroup has capacity. And has ability to get/remove the biggest batch
    Map<Comparable, BatchStatement> batchGroup;

    Session connection;

    int batchSize = 1000;

    AsyncExecutor asyncExecutor;

    boolean isAsync = false;

    KeyGenerator keyGenerator;

    public BatchCacheExecutor(Session connection, String keyspace, String table, List<String>
            columns,
                              BatchGroupingKey batchGroupingKey, int batchSize, boolean isAsync,
                              int concurrentWriter) {
        keyGenerator = new KeyGenerator(connection, keyspace, table, columns, batchGroupingKey);
        this.connection = connection;

        this.batchSize = batchSize;

        this.isAsync = isAsync;
        asyncExecutor = new AsyncExecutor(concurrentWriter);
        batchGroup = new HashMap<>();
    }

    public void addOrExecBatch(BoundStatement boundStatement) throws Exception {
        Comparable key = keyGenerator.genKey(boundStatement);
        if (batchGroup.get(key) == null) {
            BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
            batch.add(boundStatement);
            batchGroup.put(key, batch);
        } else {
            BatchStatement batch = batchGroup.get(key);
            batch.add(boundStatement);
            if (batch.size() >= batchSize) {
                execute(batch);
                batchGroup.remove(key);
            }
        }
    }

    private void execute(BatchStatement batchStatement) throws Exception {
        if (isAsync) {
            asyncExecutor.executeAsync(connection, batchStatement);
        } else {
            connection.execute(batchStatement);
        }
        batchStatement.clear();
    }

    public void endBatch() throws Exception {
        for (BatchStatement batchStatement : batchGroup.values()) {
            if (batchStatement.size() > 0) {
                execute(batchStatement);
            }
        }
        batchGroup.clear();
        if (isAsync) {
            asyncExecutor.waitForCurrentlyExecutingTasks();
        }
    }
}
