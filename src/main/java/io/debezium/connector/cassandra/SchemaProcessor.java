/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.config.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.SchemaChangeListener;
import com.datastax.driver.core.TableMetadata;

import io.debezium.connector.SourceInfoStructMaker;

/**
 * The schema processor is responsible for periodically
 * refreshing the table schemas in Cassandra. Cassandra
 * CommitLog does not provide schema change as events,
 * so we pull the schema regularly for updates.
 */
public class SchemaProcessor extends AbstractProcessor {

    private static final Logger logger = LoggerFactory.getLogger(SchemaProcessor.class);

    private static final String NAME = "Schema Processor";
    private final SchemaHolder schemaHolder;
    private final CassandraClient cassandraClient;
    private final String kafkaTopicPrefix;
    private final SourceInfoStructMaker sourceInfoStructMaker;
    private final SchemaChangeListener schemaChangeListener;

    public SchemaProcessor(CassandraConnectorContext context) {
        super(NAME, context.getCassandraConnectorConfig().schemaPollInterval());
        schemaHolder = context.getSchemaHolder();
        this.cassandraClient = context.getCassandraClient();
        this.kafkaTopicPrefix = schemaHolder.kafkaTopicPrefix;
        this.sourceInfoStructMaker = schemaHolder.sourceInfoStructMaker;

        schemaChangeListener = new NoOpSchemaChangeListener() {
            @Override
            public void onTableAdded(final TableMetadata table) {
                logger.info(String.format("Table %s.%s detected to be added!", table.getKeyspace().getName(), table.getName()));
                schemaHolder.tableToKVSchemaMap.put(new KeyspaceTable(table),
                        new SchemaHolder.KeyValueSchema(kafkaTopicPrefix, table, sourceInfoStructMaker));
                // }
            }

            @Override
            public void onTableRemoved(final TableMetadata table) {
                logger.info(String.format("Table %s.%s detected to be removed!", table.getKeyspace().getName(), table.getName()));
                schemaHolder.tableToKVSchemaMap.remove(new KeyspaceTable(table));
            }

            @Override
            public void onTableChanged(final TableMetadata current, final TableMetadata previous) {
                logger.info(String.format("Detected change in %s.%s", current.getKeyspace().getName(), current.getName()));
                schemaHolder.tableToKVSchemaMap.put(new KeyspaceTable(current),
                        new SchemaHolder.KeyValueSchema(kafkaTopicPrefix, current, sourceInfoStructMaker));
            }
        };
    }

    private List<TableMetadata> getCdcEnabledTableMetadataList() {
        return cassandraClient.getCluster().getMetadata().getKeyspaces().stream()
                .map(KeyspaceMetadata::getTables)
                .flatMap(Collection::stream)
                .filter(tm -> tm.getOptions().isCDC())
                .collect(Collectors.toList());
    }

    @Override
    public void initialize() {
        final Map<KeyspaceTable, SchemaHolder.KeyValueSchema> cdcTables = getCdcEnabledTableMetadataList()
                .stream()
                .collect(toMap(KeyspaceTable::new,
                        tableMetadata -> new SchemaHolder.KeyValueSchema(kafkaTopicPrefix, tableMetadata, sourceInfoStructMaker)));

        logger.info("Adding cdc-enabled tables {}", cdcTables.keySet().stream().map(KeyspaceTable::toString).collect(joining(",")));
        schemaHolder.tableToKVSchemaMap.putAll(cdcTables);

        logger.info("Registering schema change listener ...");
        cassandraClient.getCluster().register(schemaChangeListener);
    }

    @Override
    public void process() {
    }

    @Override
    public void destroy() {
        logger.info("Unregistering schema change listener ...");
        cassandraClient.getCluster().unregister(schemaChangeListener);
        logger.info("Clearing cdc keyspace / table map ... ");
        schemaHolder.tableToKVSchemaMap.clear();
    }

    public static final class SchemaReinitializer {
        public static synchronized void reinitialize() {
            try {
                // give it some time to flush system table to disk so we can read them again
                Thread.sleep(5000);
                clearWithoutAnnouncing();
                Schema.instance.loadFromDisk(false);
            }
            catch (final Throwable ex) {
                logger.info("Error in reinitialization method", ex);
            }
        }

        public static synchronized void clearWithoutAnnouncing() {
            for (String keyspaceName : Schema.instance.getNonSystemKeyspaces()) {
                org.apache.cassandra.schema.KeyspaceMetadata ksm = Schema.instance.getKSMetaData(keyspaceName);
                ksm.tables.forEach(view -> Schema.instance.unload(view));
                // this method does not exist
                // ksm.views.forEach(view -> Schema.instance.unload(view));
                Schema.instance.clearKeyspaceMetadata(ksm);
            }
        }
    }
}
