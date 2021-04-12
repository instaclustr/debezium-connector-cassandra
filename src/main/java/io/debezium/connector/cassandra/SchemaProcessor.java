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

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.SchemaChangeListener;
import com.datastax.driver.core.TableMetadata;
import io.debezium.connector.SourceInfoStructMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            /**
             * We add only tables which are cdc enabled
             *
             * @param table table to be added
             */
            @Override
            public void onTableAdded(final TableMetadata table) {
                if (table.getOptions().isCDC()) {
                    logger.info(String.format("CDC-enabled table %s.%s detected to be added!", table.getKeyspace().getName(), table.getName()));
                    schemaHolder.add(new KeyspaceTable(table),
                            new SchemaHolder.KeyValueSchema(kafkaTopicPrefix, table, sourceInfoStructMaker));
                }
            }

            @Override
            public void onTableRemoved(final TableMetadata table) {
                logger.info(String.format("Table %s.%s detected to be removed!", table.getKeyspace().getName(), table.getName()));
                schemaHolder.remove(new KeyspaceTable(table));
            }

            /**
             * If a table is changed, it does not mean it's cdc state have to change, there might be just changes in columns ...
             *
             * However if cdc is not enabled in the current table, we just need to remove it completely
             * If cdc is enabled, we just add it (or effectively replace, if it is already there)
             *
             * @param current current view of a table schema
             * @param previous previos view of a table schema
             */
            @Override
            public void onTableChanged(final TableMetadata current, final TableMetadata previous) {
                logger.info(String.format("Detected alternation in schema of %s.%s (previous cdc = %s, current cdc = %s)",
                        current.getKeyspace().getName(),
                        current.getName(),
                        previous.getOptions().isCDC(),
                        current.getOptions().isCDC()));

                if (current.getOptions().isCDC()) {
                    schemaHolder.add(new KeyspaceTable(current),
                            new SchemaHolder.KeyValueSchema(kafkaTopicPrefix, current, sourceInfoStructMaker));
                }
                else {
                    schemaHolder.remove(new KeyspaceTable(current));
                }
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
        cdcTables.forEach(schemaHolder::add);

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
}
