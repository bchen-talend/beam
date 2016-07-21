package org.apache.beam.sdk.io.cassandra.write;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;

import java.util.List;

/**
 *
 */
public class BatchExecutor {
    Session connection;
    BatchStatement batchStatement;

    int batchSize = 1000;

    AsyncExecutor asyncExecutor;

    boolean isAsync = false;

    KeyGenerator keyGenerator;

    Comparable lastKey;
    Comparable currentKey;

    public BatchExecutor(Session connection, String keyspace, String table, List<String> columns,
                         BatchGroupingKey batchGroupingKey, int batchSize, boolean isAsync, int
                                 concurrentWriter) {
        keyGenerator = new KeyGenerator(connection, keyspace, table, columns, batchGroupingKey);
        this.connection = connection;

        this.batchSize = batchSize;

        this.isAsync = isAsync;
        asyncExecutor = new AsyncExecutor(concurrentWriter);

        batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
    }

    private void execute() throws Exception {
        if (isAsync) {
            asyncExecutor.executeAsync(connection, batchStatement);
        } else {
            connection.execute(batchStatement);
        }
        batchStatement.clear();
    }

    public void addOrExecBatch(BoundStatement boundStatement) throws Exception {
        currentKey = keyGenerator.genKey(boundStatement);

        if (batchStatement.size() > 0 && lastKey != null && lastKey.compareTo(currentKey) != 0) {
            execute();
        }

        lastKey = currentKey;

        batchStatement.add(boundStatement);
        if (batchStatement.size() >= batchSize) {
            execute();
        }
    }

    public void endBatch() throws Exception {
        if (batchStatement.size() > 0) {
            execute();
        }
        if (isAsync) {
            asyncExecutor.waitForCurrentlyExecutingTasks();
        }
    }
}
