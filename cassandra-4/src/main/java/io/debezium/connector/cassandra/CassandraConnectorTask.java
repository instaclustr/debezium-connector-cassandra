/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;

/**
 * A task that reads Cassandra commit log in CDC directory and generate corresponding data
 * change events which will be emitted to Kafka. If the table has not been bootstrapped,
 * this task will also take a snapshot of existing data in the database and convert each row
 * into a change event as well.
 */
public class CassandraConnectorTask extends AbstractCassandraConnectorTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraConnectorTask.class);

    public static void main(String[] args) throws Exception {

        final SchemaLoader schemaLoader = new SchemaLoader() {
            @Override
            public void load(String cassandraYaml) {
                System.setProperty("cassandra.config", "file:///" + cassandraYaml);
                if (!DatabaseDescriptor.isDaemonInitialized() && !DatabaseDescriptor.isToolInitialized()) {
                    DatabaseDescriptor.toolInitialization();
                    Schema.instance.loadFromDisk(false);
                }
            }
        };

        final SchemaChangeListenerProvider schemaChangeListenerProvider = new SchemaChangeListenerProvider() {
            @Override
            public AbstractSchemaChangeListener provide(CassandraConnectorConfig config) {
                return new Cassandra4SchemaChangeListener(config.kafkaTopicPrefix(),
                        config.getSourceInfoStructMaker(),
                        new SchemaHolder());
            }
        };

        AbstractCassandraConnectorTask.main(args, config -> new CassandraConnectorTask(config,
                schemaLoader,
                schemaChangeListenerProvider));
    }

    public CassandraConnectorTask(CassandraConnectorConfig config,
                                  SchemaLoader schemaLoader,
                                  SchemaChangeListenerProvider schemaChangeListener) {
        super(config, schemaLoader, schemaChangeListener);
    }

    protected ProcessorGroup initProcessorGroup(CassandraConnectorContext taskContext) {
        try {
            ProcessorGroup processorGroup = new ProcessorGroup();
            processorGroup.addProcessor(new CommitLogProcessor(taskContext));
            processorGroup.addProcessor(new SnapshotProcessor(taskContext));
            List<ChangeEventQueue<Event>> queues = taskContext.getQueues();
            for (int i = 0; i < queues.size(); i++) {
                processorGroup.addProcessor(new QueueProcessor(taskContext, i));
            }
            if (taskContext.getCassandraConnectorConfig().postProcessEnabled()) {
                processorGroup.addProcessor(new CommitLogPostProcessor(taskContext));
            }
            LOGGER.info("Initialized Processor Group.");
            return processorGroup;
        }
        catch (Exception e) {
            throw new CassandraConnectorTaskException("Failed to initialize Processor Group.", e);
        }
    }

    @Override
    protected Logger logger() {
        return LOGGER;
    }
}
