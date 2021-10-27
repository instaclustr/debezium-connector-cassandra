/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.PingServlet;

import io.debezium.config.Configuration;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorConfigException;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;
import io.debezium.connector.cassandra.network.BuildInfoServlet;

public abstract class AbstractCassandraConnectorTask {

    public static final MetricRegistry METRIC_REGISTRY_INSTANCE = new MetricRegistry();

    private final CassandraConnectorConfig config;
    private CassandraConnectorContext taskContext;
    private ProcessorGroup processorGroup;
    private Server httpServer;
    private JmxReporter jmxReporter;
    private SchemaLoader schemaLoader;
    private SchemaChangeListenerProvider schemaChangeListenerProvider;

    protected static void main(String[] args,
                               Function<CassandraConnectorConfig, AbstractCassandraConnectorTask> taskRunner)
            throws Exception {
        if (args.length == 0) {
            throw new CassandraConnectorConfigException("CDC config file is required");
        }

        String configPath = args[0];
        try (FileInputStream fis = new FileInputStream(configPath)) {
            CassandraConnectorConfig config = new CassandraConnectorConfig(Configuration.load(fis));
            taskRunner.apply(config).run();
        }
    }

    public AbstractCassandraConnectorTask(CassandraConnectorConfig config,
                                          SchemaLoader schemaLoader,
                                          SchemaChangeListenerProvider schemaChangeListener) {
        this.config = config;
        this.schemaLoader = schemaLoader;
        this.schemaChangeListenerProvider = schemaChangeListener;
    }

    protected abstract Logger logger();

    protected abstract ProcessorGroup initProcessorGroup(CassandraConnectorContext taskContext);

    private void initJmxReporter(String domain) {
        jmxReporter = JmxReporter.forRegistry(METRIC_REGISTRY_INSTANCE).inDomain(domain).build();
    }

    private HealthCheckRegistry registerHealthCheck() {
        CassandraConnectorTaskHealthCheck healthCheck = new CassandraConnectorTaskHealthCheck(processorGroup, taskContext.getCassandraClient());
        HealthCheckRegistry healthCheckRegistry = new HealthCheckRegistry();
        healthCheckRegistry.register("cassandra-cdc-health-check", healthCheck);
        return healthCheckRegistry;
    }

    private static Map<String, String> getBuildInfoMap(Class<?> clazz) {
        Map<String, String> buildInfo = new HashMap<>();
        buildInfo.put("version", clazz.getPackage().getImplementationVersion());
        buildInfo.put("service_name", clazz.getPackage().getImplementationTitle());
        return buildInfo;
    }

    void run() throws Exception {
        try {
            if (!config.validateAndRecord(config.VALIDATION_FIELDS, logger()::error)) {
                throw new CassandraConnectorConfigException("Failed to start connector with invalid configuration " +
                        "(see logs for actual errors)");
            }

            logger().info("Initializing Cassandra connector task context ...");
            taskContext = new CassandraConnectorContext(config, schemaLoader, schemaChangeListenerProvider);

            logger().info("Starting processor group ...");
            processorGroup = initProcessorGroup(taskContext);
            processorGroup.start();

            logger().info("Starting HTTP server ...");
            initHttpServer();
            httpServer.start();

            logger().info("Starting JMX reporter ...");
            initJmxReporter(config.connectorName());
            jmxReporter.start();

            while (processorGroup.isRunning()) {
                Thread.sleep(1000);
            }
        }
        finally {
            stopAll();
        }
    }

    private void initHttpServer() {
        int httpPort = config.httpPort();
        logger().debug("HTTP port is {}", httpPort);
        httpServer = new Server(httpPort);

        ServletContextHandler contextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        contextHandler.setContextPath("/");
        httpServer.setHandler(contextHandler);

        contextHandler.addServlet(new ServletHolder(new PingServlet()), "/ping");
        contextHandler.addServlet(new ServletHolder(new BuildInfoServlet(getBuildInfoMap(this.getClass()))), "/buildinfo");
        contextHandler.addServlet(new ServletHolder(new MetricsServlet(METRIC_REGISTRY_INSTANCE)), "/metrics");
        contextHandler.addServlet(new ServletHolder(new HealthCheckServlet(registerHealthCheck())), "/health");
    }

    private void stopAll() throws Exception {
        logger().info("Stopping Cassandra Connector Task ...");
        if (processorGroup != null) {
            processorGroup.terminate();
            logger().info("Terminated processor group.");
        }

        if (httpServer != null) {
            httpServer.stop();
            logger().info("Stopped HTTP server.");
        }

        if (jmxReporter != null) {
            jmxReporter.stop();
            logger().info("Stopped JMX reporter.");
        }

        if (taskContext != null) {
            taskContext.cleanUp();
            logger().info("Cleaned up Cassandra connector context.");
        }
        logger().info("Stopped Cassandra Connector Task.");
    }

    /**
     * A processor group consist of one or more processors; each processor will be running on a separate thread.
     * The processors are interdependent of one another: if one of the processors is stopped, all other processors
     * will be signaled to stop as well.
     */
    public static class ProcessorGroup {

        private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorGroup.class);

        private final Set<AbstractProcessor> processors;
        private ExecutorService executorService;

        ProcessorGroup() {
            this.processors = new HashSet<>();
        }

        public boolean isRunning() {
            for (AbstractProcessor processor : processors) {
                if (!processor.isRunning()) {
                    return false;
                }
            }
            return true;
        }

        void addProcessor(AbstractProcessor processor) {
            processors.add(processor);
        }

        void start() {
            executorService = Executors.newFixedThreadPool(processors.size());
            for (AbstractProcessor processor : processors) {
                Runnable runnable = () -> {
                    try {
                        processor.initialize();
                        processor.start();
                    }
                    catch (Exception e) {
                        LOGGER.error("Encountered exception while running {}; stopping all processors.", processor.getName(), e);
                        try {
                            stopProcessors();
                        }
                        catch (Exception e2) {
                            LOGGER.error("Encountered exceptions while stopping all processors", e2);
                        }
                    }
                };
                executorService.submit(runnable);
            }
        }

        void terminate() {
            LOGGER.info("Terminating processor group ...");
            try {
                stopProcessors();
                if (executorService != null && !executorService.isShutdown()) {
                    executorService.shutdown();
                    if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                        executorService.shutdownNow();
                    }
                }
            }
            catch (Exception e) {
                throw new CassandraConnectorTaskException("Failed to terminate processor group.", e);
            }
        }

        private void stopProcessors() throws Exception {
            for (AbstractProcessor processor : processors) {
                processor.stop();
                processor.destroy();
            }
        }
    }
}
