/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static com.google.common.collect.ImmutableMap.of;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.github.nosan.embedded.cassandra.CassandraBuilder;
import com.github.nosan.embedded.cassandra.Version;
import com.github.nosan.embedded.cassandra.commons.ClassPathResource;

import io.debezium.config.Configuration;
import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;

public abstract class EmbeddedCassandraConnectorTestBase {

    public static final String CASSANDRA_3_VERSION = System.getProperty("cassandra3.version", "3.11.10");

    public static final String TEST_CONNECTOR_NAME = "cassandra-01";
    public static final String TEST_CASSANDRA_YAML_CONFIG = "cassandra-unit.yaml";
    public static final String TEST_CASSANDRA_HOSTS = "127.0.0.1";
    public static final int TEST_CASSANDRA_PORT = 9042;
    public static final String TEST_KAFKA_SERVERS = "localhost:9092";
    public static final String TEST_SCHEMA_REGISTRY_URL = "http://localhost:8081";
    public static final String TEST_KAFKA_TOPIC_PREFIX = "test_topic";

    public static final String TEST_KEYSPACE_NAME = "test_keyspace";

    public static Cassandra cassandra;
    private static Path cassandraDir;

    private KafkaCluster kafkaCluster;
    private File kafkaDataDir;

    @BeforeClass
    public static void setUpClass() throws Exception {
        cassandraDir = Testing.Files.createTestingDirectory("embeddedCassandra").toPath();
        cassandra = getCassandra(cassandraDir, CASSANDRA_3_VERSION);
        cassandra.start();
        waitForCql();
        createTestKeyspace();
    }

    @AfterClass
    public static void tearDownClass() {
        destroyTestKeyspace();
        cassandra.stop();
        Testing.Files.delete(cassandraDir);
    }

    @Before
    public void beforeEach() throws Exception {
        kafkaDataDir = Testing.Files.createTestingDirectory("kafkaCluster");
        Testing.Files.delete(kafkaDataDir);
        kafkaCluster = new KafkaCluster().usingDirectory(kafkaDataDir)
                .deleteDataUponShutdown(true)
                .addBrokers(1)
                .withPorts(2181, 9092)
                .startup();
    }

    @After
    public void afterEach() {
        kafkaCluster.shutdown();
        Testing.Files.delete(kafkaDataDir);
    }

    protected static void destroyTestKeyspace() {
        if (cassandra.isRunning()) {
            try (CqlSession session = CqlSession.builder().build()) {
                session.execute(SchemaBuilder.dropKeyspace(TEST_KEYSPACE_NAME).build());
            }
        }
        else {
            throw new IllegalStateException("Cassandra is not started yet or it was already stopped!");
        }
    }

    protected static void createTestKeyspace() {
        if (cassandra.isRunning()) {
            try (CqlSession session = CqlSession.builder().build()) {
                session.execute(SchemaBuilder.createKeyspace(TEST_KEYSPACE_NAME)
                        .ifNotExists()
                        .withNetworkTopologyStrategy(of("datacenter1", 1))
                        .build());
            }
        }
        else {
            throw new IllegalStateException("Cassandra is not started yet or it was already stopped!");
        }
    }

    protected static List<String> getTables(CqlSession session) {
        return session.getMetadata()
                .getKeyspace(TEST_KEYSPACE_NAME).get()
                .getTables()
                .values()
                .stream()
                .map(tmd -> tmd.getName().toString())
                .collect(Collectors.toList());
    }

    protected static void truncateTestKeyspaceTableData() {
        try (CqlSession session = CqlSession.builder().build()) {
            for (String table : getTables(session)) {
                session.execute(SimpleStatement.newInstance(String.format("TRUNCATE %s.%s", TEST_KEYSPACE_NAME, table)));
            }
        }
    }

    protected static void deleteTestKeyspaceTables() {
        try (CqlSession session = CqlSession.builder().build()) {
            for (String table : getTables(session)) {
                session.execute(SimpleStatement.newInstance(String.format("DROP TABLE IF EXISTS %s.%s", TEST_KEYSPACE_NAME, table)));
            }
        }
    }

    protected static CassandraConnectorContext generateTaskContext(Configuration configuration) throws Exception {
        return new CassandraConnectorContext(new CassandraConnectorConfig(configuration),
                new CassandraConnectorTask.Cassandra3SchemaLoader(),
                new CassandraConnectorTask.Cassandra3SchemaChangeListenerProvider());
    }

    protected static CassandraConnectorContext generateTaskContext() throws Exception {
        return generateTaskContext(Configuration.from(generateDefaultConfigMap()));
    }

    protected static CassandraConnectorContext generateTaskContext(Map<String, Object> configs) throws Exception {
        return generateTaskContext(Configuration.from(configs));
    }

    /**
     * Delete all files in offset backing store directory
     */
    protected static void deleteTestOffsets(CassandraConnectorContext context) throws IOException {
        String offsetDirPath = context.getCassandraConnectorConfig().offsetBackingStoreDir();
        File offsetDir = new File(offsetDirPath);
        if (offsetDir.isDirectory()) {
            File[] files = offsetDir.listFiles();
            if (files != null) {
                for (File f : files) {
                    Files.delete(f.toPath());
                }
            }
        }
    }

    /**
     * Return the full name of the test table in the form of <keyspace>.<table>
     */
    protected static String keyspaceTable(String tableName) {
        return TEST_KEYSPACE_NAME + "." + tableName;
    }

    /**
     * Generate a commit log file with current timestamp in memory
     */
    protected static File generateCommitLogFile() {
        File cdcDir = new File(DatabaseDescriptor.getCDCLogLocation());
        long ts = System.currentTimeMillis();
        return Paths.get(cdcDir.getAbsolutePath(), "CommitLog-6-" + ts + ".log").toFile();
    }

    /**
     * Generate commit log files in directory
     */
    protected static void populateFakeCommitLogsForDirectory(int numOfFiles, File directory) throws IOException {
        if (directory.exists() && !directory.isDirectory()) {
            throw new IOException(directory + " is not a directory");
        }
        if (!directory.exists() && !directory.mkdir()) {
            throw new IOException("Cannot create directory " + directory);
        }
        clearCommitLogFromDirectory(directory, true);
        long prefix = System.currentTimeMillis();
        for (int i = 0; i < numOfFiles; i++) {
            long ts = prefix + i;
            Path path = Paths.get(directory.getAbsolutePath(), "CommitLog-6-" + ts + ".log");
            boolean success = path.toFile().createNewFile();
            if (!success) {
                throw new IOException("Failed to create new commit log for testing");
            }
        }
    }

    /**
     * Delete all commit log files in directory
     */
    protected static void clearCommitLogFromDirectory(File directory, boolean recursive) throws IOException {
        if (!directory.exists() || !directory.isDirectory()) {
            throw new IOException(directory + " is not a valid directory");
        }

        File[] commitLogs = CommitLogUtil.getCommitLogs(directory);
        for (File commitLog : commitLogs) {
            CommitLogUtil.deleteCommitLog(commitLog);
        }

        if (recursive) {
            File[] directories = directory.listFiles(File::isDirectory);
            if (directories != null) {
                for (File dir : directories) {
                    clearCommitLogFromDirectory(dir, true);
                }
            }
        }
    }

    protected static void runCql(String statement) {
        runCql(SimpleStatement.builder(statement).build());
    }

    protected static void runCql(SimpleStatement statement) {
        if (cassandra.isRunning()) {
            try (CqlSession session = CqlSession.builder().build()) {
                session.execute(statement);
            }
        }
        else {
            throw new IllegalStateException("Cassandra is not started yet or it was already stopped!");
        }
    }

    protected static Cassandra getCassandra(final Path cassandraHome, final String version) {
        return new CassandraBuilder().version(Version.parse(version))
                .jvmOptions("-Dcassandra.ring_delay_ms=1000", "-Xms1g", "-Xmx1g")
                .workingDirectory(() -> cassandraHome)
                .configFile(new ClassPathResource("cassandra-unit.yaml")).build();
    }

    protected static Properties generateDefaultConfigMap() throws IOException {
        Properties props = new Properties();
        props.put(CassandraConnectorConfig.CONNECTOR_NAME.name(), TEST_CONNECTOR_NAME);
        props.put(CassandraConnectorConfig.CASSANDRA_CONFIG.name(), Paths.get("src/test/resources/cassandra-unit-for-context.yaml").toAbsolutePath().toString());
        props.put(CassandraConnectorConfig.KAFKA_TOPIC_PREFIX.name(), TEST_KAFKA_TOPIC_PREFIX);
        props.put(CassandraConnectorConfig.CASSANDRA_HOSTS.name(), TEST_CASSANDRA_HOSTS);
        props.put(CassandraConnectorConfig.CASSANDRA_PORT.name(), String.valueOf(TEST_CASSANDRA_PORT));
        props.put(CassandraConnectorConfig.OFFSET_BACKING_STORE_DIR.name(), Files.createTempDirectory("offset").toString());
        props.put(CassandraConnectorConfig.KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TEST_KAFKA_SERVERS);
        props.put(CassandraConnectorConfig.COMMIT_LOG_RELOCATION_DIR.name(), Files.createTempDirectory("cdc_raw_relocation").toString());
        return props;
    }

    protected static HashMap<String, Object> propertiesForContext() throws IOException {
        return new HashMap<String, Object>() {
            {
                put(CassandraConnectorConfig.CONNECTOR_NAME.name(), TEST_CONNECTOR_NAME);
                put(CassandraConnectorConfig.CASSANDRA_CONFIG.name(), Paths.get("src/test/resources/cassandra-unit-for-context.yaml").toAbsolutePath().toString());
                put(CassandraConnectorConfig.KAFKA_TOPIC_PREFIX.name(), TEST_KAFKA_TOPIC_PREFIX);
                put(CassandraConnectorConfig.CASSANDRA_HOSTS.name(), TEST_CASSANDRA_HOSTS);
                put(CassandraConnectorConfig.CASSANDRA_PORT.name(), String.valueOf(TEST_CASSANDRA_PORT));
                put(CassandraConnectorConfig.OFFSET_BACKING_STORE_DIR.name(), Files.createTempDirectory("offset").toString());
                put(CassandraConnectorConfig.KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TEST_KAFKA_SERVERS);
                put(CassandraConnectorConfig.COMMIT_LOG_RELOCATION_DIR.name(), Files.createTempDirectory("cdc_raw_relocation").toString());
            }
        };
    }

    protected static void waitForCql() {
        await()
                .pollInterval(10, SECONDS)
                .pollInSameThread()
                .timeout(1, MINUTES)
                .until(() -> {
                    try (final CqlSession ignored = CqlSession.builder().build()) {
                        return true;
                    }
                    catch (Exception ex) {
                        return false;
                    }
                });
    }
}
