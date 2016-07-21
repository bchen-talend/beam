package org.apache.beam.sdk.io.cassandra.write;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * Async executor for cassandra batch writer.
 */
public class AsyncExecutor {
    Semaphore semaphore;

    List<ResultSetFuture> pendingFutures;

    public AsyncExecutor(int concurrentWriter) {
        semaphore = new Semaphore(concurrentWriter);
        pendingFutures = new ArrayList<>();
    }

    public void executeAsync(Session connection, BatchStatement batchStatement) throws Exception {
        semaphore.acquire();

        checkPendingFutures();

        ResultSetFuture resultSetFuture = connection.executeAsync(batchStatement);

        pendingFutures.add(resultSetFuture);

        Futures.addCallback(resultSetFuture, new FutureCallback<ResultSet>() {
            void release() {
                semaphore.release();
            }

            @Override
            public void onSuccess(ResultSet rows) {
                release();
            }

            @Override
            public void onFailure(Throwable throwable) {
                release();
                //TODO how to handle exception
            }
        });
    }

    private void checkPendingFutures() throws Exception {
        List<ResultSetFuture> doneList = null;
        for (ResultSetFuture pendingFuture : pendingFutures) {
            if (pendingFuture.isDone()) {
                if (doneList == null) {
                    doneList = new ArrayList<>();
                }
                doneList.add(pendingFuture);
            }
        }
        if (doneList != null) {
            pendingFutures.removeAll(doneList);
        }
    }

    public void waitForCurrentlyExecutingTasks() throws Exception {
        checkPendingFutures();
        for (ResultSetFuture pendingFuture : pendingFutures) {
            pendingFuture.getUninterruptibly();
        }
    }

}
