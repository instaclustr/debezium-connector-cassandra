/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.lang.reflect.Method;
import java.util.Optional;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.KeyspaceParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.AggregateMetadata;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.FunctionMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.MaterializedViewMetadata;
import com.datastax.driver.core.SchemaChangeListener;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;

import io.debezium.connector.SourceInfoStructMaker;

/**
 * Listening to schema changes in Cassandra DB and caches the key and value schema for all CDC-enabled tables.
 * This cache gets updated whenever there's a schema change in Cassandra DB
 */
public class SchemaHolder extends AbstractSchemaHolder {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaHolder.class);

    private final SchemaChangeListener schemaChangeListener;

    public SchemaHolder(CassandraClient cassandraClient, String kafkaTopicPrefix, SourceInfoStructMaker<SourceInfo> sourceInfoStructMaker) {
        super(cassandraClient, kafkaTopicPrefix, sourceInfoStructMaker);
        this.schemaChangeListener = new CassandraSchemaChangeListener(kafkaTopicPrefix, sourceInfoStructMaker);
        initialize(schemaChangeListener);
    }

    @Override
    public void close() {
        cassandraClient.getCluster().unregister(schemaChangeListener);
        logger().info("Closed SchemaHolder.");
    }

    @Override
    protected Logger logger() {
        return LOGGER;
    }

    class CassandraSchemaChangeListener implements SchemaChangeListener {

        private final String kafkaTopicPrefix;
        private final SourceInfoStructMaker<SourceInfo> sourceInfoStructMaker;

        public CassandraSchemaChangeListener(String kafkaTopicPrefix, SourceInfoStructMaker<SourceInfo> sourceInfoStructMaker) {
            this.kafkaTopicPrefix = kafkaTopicPrefix;
            this.sourceInfoStructMaker = sourceInfoStructMaker;
        }

        @Override
        public void onKeyspaceAdded(final KeyspaceMetadata keyspaceMetadata) {
            try {
                Schema.instance.setKeyspaceMetadata(org.apache.cassandra.schema.KeyspaceMetadata.create(
                        keyspaceMetadata.getName(),
                        KeyspaceParams.create(keyspaceMetadata.isDurableWrites(),
                                keyspaceMetadata.getReplication())));
                Keyspace.openWithoutSSTables(keyspaceMetadata.getName());
                logger().info("Added keyspace [{}] to schema instance.", keyspaceMetadata.asCQLQuery());
            }
            catch (Exception e) {
                logger().warn("Error happened while adding the keyspace {} to schema instance.", keyspaceMetadata.getName(), e);
            }
        }

        @Override
        public void onKeyspaceChanged(final KeyspaceMetadata current, final KeyspaceMetadata previous) {
            try {
                Schema.instance.updateKeyspace(current.getName(), KeyspaceParams.create(current.isDurableWrites(), current.getReplication()));
                logger().info("Updated keyspace [{}] in schema instance.", current.asCQLQuery());
            }
            catch (Exception e) {
                logger().warn("Error happened while updating the keyspace {} in schema instance.", current.getName(), e);
            }
        }

        @Override
        public void onKeyspaceRemoved(final KeyspaceMetadata keyspaceMetadata) {
            try {
                Schema.instance.clearKeyspaceMetadata(org.apache.cassandra.schema.KeyspaceMetadata.create(
                        keyspaceMetadata.getName(),
                        KeyspaceParams.create(keyspaceMetadata.isDurableWrites(),
                                keyspaceMetadata.getReplication())));
                logger().info("Removed keyspace [{}] from schema instance.", keyspaceMetadata.asCQLQuery());
            }
            catch (Exception e) {
                logger().warn("Error happened while removing the keyspace {} from schema instance.", keyspaceMetadata.getName(), e);
            }
        }

        @Override
        public void onTableAdded(final TableMetadata tableMetadata) {
            if (tableMetadata.getOptions().isCDC()) {
                addOrUpdateTableSchema(new KeyspaceTable(tableMetadata),
                        new KeyValueSchema(kafkaTopicPrefix, tableMetadata, sourceInfoStructMaker));
            }
            try {
                logger().debug("Table {}.{} detected to be added!", tableMetadata.getKeyspace().getName(), tableMetadata.getName());
                final CFMetaData rawCFMetaData = CFMetaData.compile(tableMetadata.asCQLQuery(), tableMetadata.getKeyspace().getName());
                // we need to copy because CFMetaData.compile will generate new cfId which would not match id of old metadata
                final CFMetaData newCFMetaData = rawCFMetaData.copy(tableMetadata.getId());
                Keyspace.open(newCFMetaData.ksName).initCf(newCFMetaData, false);
                final org.apache.cassandra.schema.KeyspaceMetadata current = Schema.instance.getKSMetaData(newCFMetaData.ksName);
                if (current == null) {
                    logger().warn("Keyspace {} doesn't exist", newCFMetaData.ksName);
                    return;
                }
                if (current.tables.get(tableMetadata.getName()).isPresent()) {
                    logger().debug("Table {}.{} is already added!", tableMetadata.getKeyspace(), tableMetadata.getName());
                    return;
                }
                org.apache.cassandra.schema.KeyspaceMetadata transformed = current.withSwapped(current.tables.with(newCFMetaData));
                Schema.instance.setKeyspaceMetadata(transformed);
                Schema.instance.load(newCFMetaData);
                logger().info("Added table [{}] to schema instance.", tableMetadata.asCQLQuery());
            }
            catch (Exception e) {
                logger().warn("Error happened while adding table {}.{} to schema instance.", tableMetadata.getKeyspace(), tableMetadata.getName(), e);
            }
        }

        @Override
        public void onTableRemoved(final TableMetadata table) {
            if (table.getOptions().isCDC()) {
                removeTableSchema(new KeyspaceTable(table));
            }
            try {
                final String ksName = table.getKeyspace().getName();
                final String tableName = table.getName();
                logger().debug("Table {}.{} detected to be removed!", ksName, tableName);
                final org.apache.cassandra.schema.KeyspaceMetadata oldKsm = Schema.instance.getKSMetaData(ksName);
                if (oldKsm == null) {
                    logger().warn("KeyspaceMetadata for keyspace {} is not found!", ksName);
                    return;
                }
                final ColumnFamilyStore cfs = Keyspace.openWithoutSSTables(ksName).getColumnFamilyStore(tableName);
                if (cfs == null) {
                    logger().warn("ColumnFamilyStore for {}.{} is not found!", ksName, tableName);
                    return;
                }
                // make sure all the indexes are dropped, or else.
                cfs.indexManager.markAllIndexesRemoved();
                // reinitialize the keyspace.
                final Optional<CFMetaData> cfm = oldKsm.tables.get(tableName);

                final Method unregisterMBeanMethod = ColumnFamilyStore.class.getDeclaredMethod("unregisterMBean");
                unregisterMBeanMethod.setAccessible(true);
                unregisterMBeanMethod.invoke(cfs);

                if (cfm.isPresent()) {
                    final org.apache.cassandra.schema.KeyspaceMetadata newKsm = oldKsm.withSwapped(oldKsm.tables.without(tableName));
                    Schema.instance.unload(cfm.get());
                    Schema.instance.setKeyspaceMetadata(newKsm);
                    logger().info("Removed table [{}] from schema instance.", table.asCQLQuery());
                }
                else {
                    logger().warn("Table {}.{} is not present in old keyspace meta data!", ksName, tableName);
                }

            }
            catch (Exception e) {
                logger().warn("Error happened while removing table {}.{} from schema instance.", table.getKeyspace().getName(), table.getName(), e);
            }
        }

        @Override
        public void onTableChanged(final TableMetadata newTableMetadata, final TableMetadata oldTableMetaData) {
            if (newTableMetadata.getOptions().isCDC()) {
                // if it was cdc before and now it is too, add it, because its schema might change
                // however if it is CDC-enabled but it was not, update it in schema too because its cdc flag has changed
                // this basically means we add / update every time if new has cdc flag equals to true
                addOrUpdateTableSchema(new KeyspaceTable(newTableMetadata),
                        new KeyValueSchema(kafkaTopicPrefix, newTableMetadata, sourceInfoStructMaker));
            }
            else if (oldTableMetaData.getOptions().isCDC()) {
                // if new table is not on cdc anymore, and we see the old one was, remove it
                removeTableSchema(new KeyspaceTable(newTableMetadata));
            }
            try {
                logger().debug("Detected alternation in schema of {}.{} (previous cdc = {}, current cdc = {})",
                        newTableMetadata.getKeyspace().getName(),
                        newTableMetadata.getName(),
                        oldTableMetaData.getOptions().isCDC(),
                        newTableMetadata.getOptions().isCDC());
                // else if it was not cdc before nor now, do nothing with schema holder
                // but add it to Cassandra for subsequent deserialization path in every case
                final CFMetaData rawNewMetadata = CFMetaData.compile(newTableMetadata.asCQLQuery(),
                        newTableMetadata.getKeyspace().getName());
                final CFMetaData oldMetadata = Schema.instance.getCFMetaData(oldTableMetaData.getKeyspace().getName(), oldTableMetaData.getName());
                // we need to copy because CFMetaData.compile will generate new cfId which would not match id of old metadata
                final CFMetaData newMetadata = rawNewMetadata.copy(oldMetadata.cfId);
                oldMetadata.apply(newMetadata);
                logger().info("Updated table [{}] in schema instance.", newTableMetadata.asCQLQuery());
            }
            catch (Exception e) {
                logger().warn("Error happened while reacting on changed table {}.{} in schema instance.", newTableMetadata.getKeyspace(), newTableMetadata.getName(), e);
            }
        }

        @Override
        public void onUserTypeAdded(final UserType type) {
            // Not relevant to Debezium Cassandra Connector
        }

        @Override
        public void onUserTypeRemoved(final UserType type) {
            // Not relevant to Debezium Cassandra Connector
        }

        @Override
        public void onUserTypeChanged(final UserType current, final UserType previous) {
            // Not relevant to Debezium Cassandra Connector
        }

        @Override
        public void onFunctionAdded(final FunctionMetadata function) {
            // Not relevant to Debezium Cassandra Connector
        }

        @Override
        public void onFunctionRemoved(final FunctionMetadata function) {
            // Not relevant to Debezium Cassandra Connector
        }

        @Override
        public void onFunctionChanged(final FunctionMetadata current, final FunctionMetadata previous) {
            // Not relevant to Debezium Cassandra Connector
        }

        @Override
        public void onAggregateAdded(final AggregateMetadata aggregate) {
            // Not relevant to Debezium Cassandra Connector
        }

        @Override
        public void onAggregateRemoved(final AggregateMetadata aggregate) {
            // Not relevant to Debezium Cassandra Connector
        }

        @Override
        public void onAggregateChanged(final AggregateMetadata current, final AggregateMetadata previous) {
            // Not relevant to Debezium Cassandra Connector
        }

        @Override
        public void onMaterializedViewAdded(final MaterializedViewMetadata view) {
            // Not relevant to Debezium Cassandra Connector
        }

        @Override
        public void onMaterializedViewRemoved(final MaterializedViewMetadata view) {
            // Not relevant to Debezium Cassandra Connector
        }

        @Override
        public void onMaterializedViewChanged(final MaterializedViewMetadata current, final MaterializedViewMetadata previous) {
            // Not relevant to Debezium Cassandra Connector
        }

        @Override
        public void onRegister(final Cluster cluster) {
            // Not relevant to Debezium Cassandra Connector
        }

        @Override
        public void onUnregister(final Cluster cluster) {
            // Not relevant to Debezium Cassandra Connector
        }
    }

}
