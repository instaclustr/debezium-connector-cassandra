/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;

/**
 * The {@link CommitLogProcessor} is used to process CommitLog in CDC directory.
 * Upon readCommitLog, it processes the entire CommitLog specified in the {@link CassandraConnectorConfig}
 * and converts each row change in the commit log into a {@link Record},
 * and then emit the log via a {@link KafkaRecordEmitter}.
 */
public class CommitLogProcessor extends AbstractProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitLogProcessor.class);

    private static final String NAME = "Commit Log Processor";

    private final CassandraConnectorContext context;
    private final File cdcDir;
    private final AbstractDirectoryWatcher watcher;
    private final List<ChangeEventQueue<Event>> queues;
    private final CommitLogProcessorMetrics metrics = new CommitLogProcessorMetrics();
    private boolean initial = true;
    private final boolean errorCommitLogReprocessEnabled;
    private final CommitLogTransfer commitLogTransfer;
    private final ExecutorService executorService;

    public CommitLogProcessor(CassandraConnectorContext context) throws IOException {
        super(NAME, Duration.ZERO);
        this.context = context;
        executorService = Executors.newSingleThreadExecutor();
        queues = this.context.getQueues();
        commitLogTransfer = this.context.getCassandraConnectorConfig().getCommitLogTransfer();
        errorCommitLogReprocessEnabled = this.context.getCassandraConnectorConfig().errorCommitLogReprocessEnabled();
        cdcDir = new File(DatabaseDescriptor.getCDCLogLocation());

        watcher = new AbstractDirectoryWatcher(cdcDir.toPath(),
                this.context.getCassandraConnectorConfig().cdcDirPollInterval(),
                Collections.singleton(ENTRY_CREATE)) {
            @Override
            void handleEvent(WatchEvent<?> event, Path path) {
                if (isRunning()) {
                    if (path.getFileName().endsWith("_cdc.idx")) {
                        executorService.submit(new CommitLogProcessingRunnable(new LogicalCommitLog(path.toFile()),
                                queues,
                                metrics,
                                CommitLogProcessor.this.context));
                    }
                }
            }
        };
    }

    @Override
    public void initialize() {
        metrics.registerMetrics();
    }

    @Override
    public void destroy() {
        metrics.unregisterMetrics();
        executorService.shutdown();
        try {
            executorService.awaitTermination(1, TimeUnit.MINUTES);
        }
        catch (final Exception ex) {
            throw new RuntimeException("Unable to close executor service in CommitLogProcessor in a timely manner");
        }
    }

    @Override
    public void process() throws IOException, InterruptedException {
        LOGGER.debug("Processing commitLogFiles while initial is {}", initial);
        if (initial) {
            LOGGER.info("Reading existing commit logs in {}", cdcDir);
            File[] indexes = CommitLogUtil.getIndexes(cdcDir);
            Arrays.sort(indexes, CommitLogUtil::compareCommitLogsIndexes);
            for (File index : indexes) {
                if (isRunning()) {
                    executorService.submit(new CommitLogProcessingRunnable(new LogicalCommitLog(index),
                            queues,
                            metrics,
                            context));
                }
            }
            // If commit.log.error.reprocessing.enabled is set to true, download all error commitLog files upon starting for re-processing.
            if (errorCommitLogReprocessEnabled) {
                LOGGER.info("CommitLog Error Processing is enabled. Attempting to get all error commitLog files for re-processing.");
                commitLogTransfer.getErrorCommitLogFiles();
            }
            initial = false;
        }
        watcher.poll();
    }

    public static class LogicalCommitLog {
        CommitLogPosition commitLogPosition;
        File log;
        File index;
        long commitLogSegmentId;
        int start = 0;
        int end = 0;
        int synced = 0;
        boolean completed = false;

        public LogicalCommitLog(File index) {
            this.index = index;
            this.log = parseCommitLogName(index);
            this.commitLogSegmentId = parseSegmentId(log);
            this.commitLogPosition = new CommitLogPosition(commitLogSegmentId, 0);
        }

        public static File parseCommitLogName(File index) {
            String newFileName = index.toPath().getFileName().toString().replace("_cdc.idx", ".log");
            return index.toPath().getParent().resolve(newFileName).toFile();
        }

        public static long parseSegmentId(File logName) {
            return Long.parseLong(logName.getName().split("-")[2].split("\\.")[0]);
        }

        public boolean exists() {
            return log.exists() && index.exists();
        }

        public void parseCommitLogIndex() throws Exception {
            List<String> lines = Files.readAllLines(index.toPath(), StandardCharsets.UTF_8);
            if (lines.isEmpty()) {
                return;
            }

            synced = Integer.parseInt(lines.get(0));

            if (lines.size() == 2) {
                completed = "COMPLETED".equals(lines.get(1));
            }
        }

        @Override
        public String toString() {
            return "LogicalCommitLog{" +
                    "commitLogPosition=" + commitLogPosition +
                    ", log=" + log +
                    ", index=" + index +
                    ", commitLogSegmentId=" + commitLogSegmentId +
                    ", start=" + start +
                    ", end=" + end +
                    ", synced=" + synced +
                    ", completed=" + completed +
                    '}';
        }
    }

    public static class CommitLogProcessingRunnable implements Runnable {

        private static final Logger LOGGER = LoggerFactory.getLogger(CommitLogProcessingRunnable.class);

        private final CassandraConnectorContext context;
        private final LogicalCommitLog commitLog;
        private CommitLogReader commitLogReader;
        private final List<ChangeEventQueue<Event>> queues;
        private final CommitLogProcessorMetrics metrics;
        private final CommitLogReadHandlerImpl commitLogReadHandler;

        private final CommitLogTransfer commitLogTransfer;
        private final Set<String> erroneousCommitLogs;

        public CommitLogProcessingRunnable(final LogicalCommitLog commitLog,
                                           final List<ChangeEventQueue<Event>> queues,
                                           final CommitLogProcessorMetrics metrics,
                                           CassandraConnectorContext context) {
            this.context = context;
            this.commitLog = commitLog;
            // TODO or is it safe to reuse it?
            this.commitLogReader = new CommitLogReader();
            this.queues = queues;
            this.metrics = metrics;

            this.commitLogReadHandler = new CommitLogReadHandlerImpl(
                    this.context.getSchemaHolder(),
                    this.context.getQueues(),
                    this.context.getOffsetWriter(),
                    new RecordMaker(this.context.getCassandraConnectorConfig().tombstonesOnDelete(),
                            new Filters(this.context.getCassandraConnectorConfig().fieldExcludeList()),
                            this.context.getCassandraConnectorConfig()),
                    metrics);

            commitLogTransfer = context.getCassandraConnectorConfig().getCommitLogTransfer();
            erroneousCommitLogs = context.getErroneousCommitLogs();
        }

        @Override
        public void run() {
            if (!commitLog.exists()) {
                LOGGER.warn("Commit log " + commitLog + " does not exist or index file or log itself is missing!");
                return;
            }

            int latestKnownPosition = 0;
            boolean finished = false;

            while (!finished) {
                try {
                    commitLog.parseCommitLogIndex();
                }
                catch (final Exception ex) {
                    LOGGER.error(String.format("Unable to parse index file for commit log at %s", commitLog.log), ex);
                    break;
                }

                final OffsetPosition latestProcessedPosition = commitLogReadHandler.getLatestOffsetPosition();

                LOGGER.info(String.format("%s latest known position: %d", commitLog.log, latestKnownPosition));
                LOGGER.info(String.format("%s latest offset position: %d", commitLog.log, latestProcessedPosition == null ? -1 : latestProcessedPosition.filePosition));

                if (latestProcessedPosition != null && (latestKnownPosition == latestProcessedPosition.filePosition)) {
                    if (commitLog.completed && commitLog.synced == latestProcessedPosition.filePosition) {
                        LOGGER.info(String.format("finishing %s as the latest offset position is %s", commitLog.log, latestKnownPosition));
                        finished = true;
                        continue;
                    }
                }

                // new position to read from
                CommitLogPosition position;

                // if we have not processed any mutations yet, just start from the beginning
                // otherwise start from where we left it
                if (latestProcessedPosition == null) {
                    position = new CommitLogPosition(commitLog.commitLogSegmentId, 0);
                }
                else {
                    position = new CommitLogPosition(commitLog.commitLogSegmentId, latestProcessedPosition.filePosition);
                }

                LOGGER.info("Processing from position {}", position);
                processCommitLog(commitLog, position);
                latestKnownPosition = position.position;
            }

            commitLogReader = null;
        }

        void processCommitLog(LogicalCommitLog logicalCommitLog, CommitLogPosition position) {
            try {
                try {
                    CommitLogProcessor.LOGGER.info("Processing commit log {}", logicalCommitLog.log.toString());
                    metrics.setCommitLogFilename(logicalCommitLog.log.toString());

                    commitLogReader.readCommitLogSegment(commitLogReadHandler, logicalCommitLog.log, position, false);
                    queues.get(Math.abs(logicalCommitLog.log.getName().hashCode() % queues.size())).enqueue(new EOFEvent(logicalCommitLog.log));

                    CommitLogProcessor.LOGGER.info("Successfully processed commit log {}", logicalCommitLog.log);
                }
                catch (Exception e) {
                    if (commitLogTransfer.getClass().getName().equals(CassandraConnectorConfig.DEFAULT_COMMIT_LOG_TRANSFER_CLASS)) {
                        throw new DebeziumException(String.format("Error occurred while processing commit log %s",
                                logicalCommitLog.log), e);
                    }
                    CommitLogProcessor.LOGGER.error("Error occurred while processing commit log " + logicalCommitLog.log, e);
                    queues.get(Math.abs(logicalCommitLog.log.getName().hashCode() % queues.size())).enqueue(new EOFEvent(logicalCommitLog.log));
                    erroneousCommitLogs.add(logicalCommitLog.log.getName());
                }
                // TODO - make this configurable
                // after 10 seconds, read the index, parse offsets and process mutations again
                // this is the core difference between Debezium connector for Cassandra 3 as it would process
                // mutations only in case log file is moved to cdc dir. However, from Cassandra 4, log files are
                // hardlinked and they are increasing "on the fly". This brings us almost realtime experience.
                Thread.sleep(10000);
            }
            catch (InterruptedException e) {
                throw new CassandraConnectorTaskException(String.format(
                        "Enqueuing has been interrupted while enqueuing EOF Event for file %s", logicalCommitLog.log.getName()), e);
            }
        }
    }
}
