/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The {@link CommitLogPostProcessor} is used to post-process commit logs in the COMMIT_LOG_RELOCATION_DIR
 * once the commit log has been processed by the CommitLogProcessor. How the commit log is post-processed
 * depends on the implementation detail of the {@link CommitLogTransfer}.
 */
public class CommitLogPostProcessor extends AbstractProcessor {

    private static final String NAME = "Commit Log Post-Processor";
    private static final int THREAD_POOL_SIZE = 10;
    private static final int TERMINATION_WAIT_TIME_SECONDS = 10;

    private final ExecutorService executor;
    private final String commitLogRelocationDir;
    private final CommitLogTransfer commitLogTransfer;

    public CommitLogPostProcessor(CassandraConnectorConfig config) {
        super(NAME, config.commitLogRelocationDirPollInterval());
        this.commitLogRelocationDir = config.commitLogRelocationDir();
        this.executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        this.commitLogTransfer = config.getCommitLogTransfer();
    }

    @Override
    public void process() {
        File[] commitLogs = CommitLogUtil.getCommitLogs(Paths.get(commitLogRelocationDir, QueueProcessor.ARCHIVE_FOLDER).toFile());
        Arrays.sort(commitLogs, CommitLogUtil::compareCommitLogs);
        for (File commitLog : commitLogs) {
            if (isRunning()) {
                executor.submit(() -> commitLogTransfer.onSuccessTransfer(commitLog));
            }
        }

        File[] errCommitLogs = CommitLogUtil.getCommitLogs(Paths.get(commitLogRelocationDir, QueueProcessor.ERROR_FOLDER).toFile());
        Arrays.sort(errCommitLogs, CommitLogUtil::compareCommitLogs);
        for (File errCommitLog : errCommitLogs) {
            if (isRunning()) {
                executor.submit(() -> commitLogTransfer.onErrorTransfer(errCommitLog));
            }
        }
    }

    @Override
    public void destroy() throws Exception {
        shutDown(true);
        commitLogTransfer.destroy();

    }

    void shutDown(boolean await) {
        try {
            if (!executor.isShutdown()) {
                executor.shutdown();
                if (await) {
                    boolean terminated = executor.awaitTermination(TERMINATION_WAIT_TIME_SECONDS, TimeUnit.SECONDS);
                    if (!terminated) {
                        executor.shutdownNow();
                    }
                }
            }
        }
        catch (InterruptedException e) {
            if (!executor.isTerminated()) {
                executor.shutdownNow();
            }
        }
    }
}
