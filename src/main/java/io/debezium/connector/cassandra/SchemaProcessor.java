/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static java.util.stream.Collectors.toMap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.KeyspaceParams;
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
            public void onKeyspaceAdded(final KeyspaceMetadata keyspace) {
                Schema.instance.setKeyspaceMetadata(org.apache.cassandra.schema.KeyspaceMetadata.create(
                        keyspace.getName(),
                        KeyspaceParams.create(keyspace.isDurableWrites(),
                                keyspace.getReplication())));
                Keyspace.openWithoutSSTables(keyspace.getName());
                logger.info("added keyspace {}", keyspace.asCQLQuery());
            }

            @Override
            public void onKeyspaceChanged(final KeyspaceMetadata current, final KeyspaceMetadata previous) {
                Schema.instance.updateKeyspace(current.getName(), KeyspaceParams.create(current.isDurableWrites(), current.getReplication()));
                logger.info("updated keyspace {}", current.asCQLQuery());
            }

            @Override
            public void onKeyspaceRemoved(final KeyspaceMetadata keyspace) {
                schemaHolder.removeKeyspace(keyspace.getName());
                Schema.instance.clearKeyspaceMetadata(org.apache.cassandra.schema.KeyspaceMetadata.create(
                        keyspace.getName(),
                        KeyspaceParams.create(keyspace.isDurableWrites(),
                                keyspace.getReplication())));
                logger.info("removed keyspace {}", keyspace.asCQLQuery());
            }

            @Override
            public void onTableAdded(final TableMetadata table) {
                logger.info(String.format("Table %s.%s detected to be added!", table.getKeyspace().getName(), table.getName()));
                schemaHolder.addTable(new KeyspaceTable(table),
                        new SchemaHolder.KeyValueSchema(kafkaTopicPrefix, table, sourceInfoStructMaker));

                final CFMetaData rawCFMetaData = CFMetaData.compile(table.asCQLQuery(), table.getKeyspace().getName());
                // we need to copy because CFMetaData.compile will generate new cfId which would not match id of old metadata
                final CFMetaData newCFMetaData = rawCFMetaData.copy(table.getId());

                Keyspace.open(newCFMetaData.ksName).initCf(newCFMetaData, false);

                final org.apache.cassandra.schema.KeyspaceMetadata current = Schema.instance.getKSMetaData(newCFMetaData.ksName);
                if (current == null) {
                    throw new IllegalStateException(String.format("Keyspace %s doesn't exist", newCFMetaData.ksName));
                }

                if (current.tables.get(table.getName()).isPresent()) {
                    logger.info(String.format("table %s.%s is already added!", table.getKeyspace(), table.getName()));
                    return;
                }

                final java.util.function.Function<org.apache.cassandra.schema.KeyspaceMetadata, org.apache.cassandra.schema.KeyspaceMetadata> f = ks -> ks
                        .withSwapped(ks.tables.with(newCFMetaData));

                org.apache.cassandra.schema.KeyspaceMetadata transformed = f.apply(current);

                Schema.instance.setKeyspaceMetadata(transformed);
                Schema.instance.load(newCFMetaData);

                logger.info("added table {}", table.asCQLQuery());
            }

            @Override
            public void onTableRemoved(final TableMetadata table) {
                logger.info(String.format("Table %s.%s detected to be removed!", table.getKeyspace().getName(), table.getName()));
                schemaHolder.removeTable(new KeyspaceTable(table));

                final String ksName = table.getKeyspace().getName();
                final String tableName = table.getName();

                final org.apache.cassandra.schema.KeyspaceMetadata oldKsm = Schema.instance.getKSMetaData(table.getKeyspace().getName());

                if (oldKsm == null) {
                    throw new IllegalStateException(String.format("KeyspaceMetadata for keyspace %s is not found!", table.getKeyspace().getName()));
                }

                final ColumnFamilyStore cfs = Keyspace.openWithoutSSTables(ksName).getColumnFamilyStore(tableName);

                if (cfs == null) {
                    throw new IllegalStateException(String.format("ColumnFamilyStore for %s.%s is not found!", table.getKeyspace(), table.getName()));
                }

                // make sure all the indexes are dropped, or else.
                cfs.indexManager.markAllIndexesRemoved();

                // reinitialize the keyspace.
                final CFMetaData cfm = oldKsm.tables.get(tableName).get();
                final org.apache.cassandra.schema.KeyspaceMetadata newKsm = oldKsm.withSwapped(oldKsm.tables.without(tableName));

                Schema.instance.unload(cfm);
                Schema.instance.setKeyspaceMetadata(newKsm);

                logger.info("removed table {}", table.asCQLQuery());
            }

            @Override
            public void onTableChanged(final TableMetadata newTableMetadata, final TableMetadata oldTableMetaData) {
                logger.info(String.format("Detected alternation in schema of %s.%s (previous cdc = %s, current cdc = %s)",
                        newTableMetadata.getKeyspace().getName(),
                        newTableMetadata.getName(),
                        oldTableMetaData.getOptions().isCDC(),
                        newTableMetadata.getOptions().isCDC()));

                schemaHolder.addTable(new KeyspaceTable(newTableMetadata),
                        new SchemaHolder.KeyValueSchema(kafkaTopicPrefix, newTableMetadata, sourceInfoStructMaker));

                final CFMetaData rawNewMetadata = CFMetaData.compile(newTableMetadata.asCQLQuery(),
                        newTableMetadata.getKeyspace().getName());

                final CFMetaData oldMetadata = Schema.instance.getCFMetaData(oldTableMetaData.getKeyspace().getName(), oldTableMetaData.getName());

                // we need to copy because CFMetaData.compile will generate new cfId which would not match id of old metadata
                final CFMetaData newMetadata = rawNewMetadata.copy(oldMetadata.cfId);
                oldMetadata.apply(newMetadata);

                logger.info("changed table {}", newTableMetadata.asCQLQuery());
            }
        };
    }

    @Override
    public void initialize() {
        final Map<KeyspaceTable, SchemaHolder.KeyValueSchema> tables = getTableMetadata()
                .stream()
                .collect(toMap(KeyspaceTable::new,
                        tableMetadata -> new SchemaHolder.KeyValueSchema(kafkaTopicPrefix, tableMetadata, sourceInfoStructMaker)));

        tables.forEach(schemaHolder::addTable);

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

    private List<TableMetadata> getTableMetadata() {
        return cassandraClient.getCluster().getMetadata().getKeyspaces().stream()
                .map(KeyspaceMetadata::getTables)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }
}
