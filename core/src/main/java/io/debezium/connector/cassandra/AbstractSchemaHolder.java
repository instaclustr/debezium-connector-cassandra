/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import com.datastax.driver.core.SchemaChangeListener;
import com.datastax.driver.core.TableMetadata;

import io.debezium.connector.SourceInfoStructMaker;

public abstract class AbstractSchemaHolder {

    private final ConcurrentMap<KeyspaceTable, KeyValueSchema> tableToKVSchemaMap;
    private final String kafkaTopicPrefix;
    private final SourceInfoStructMaker<SourceInfo> sourceInfoStructMaker;
    protected final CassandraClient cassandraClient;

    public AbstractSchemaHolder(CassandraClient cassandraClient, String kafkaTopicPrefix, SourceInfoStructMaker<SourceInfo> sourceInfoStructMaker) {
        this.cassandraClient = cassandraClient;
        this.kafkaTopicPrefix = kafkaTopicPrefix;
        this.sourceInfoStructMaker = sourceInfoStructMaker;
        this.tableToKVSchemaMap = new ConcurrentHashMap<>();
    }

    public abstract void close();

    protected abstract Logger logger();

    protected void initialize(SchemaChangeListener schemaChangeListener) {
        logger().info("Initializing SchemaHolder ...");
        List<TableMetadata> cdcEnabledTableMetadataList = cassandraClient.getCdcEnabledTableMetadataList();
        for (TableMetadata tm : cdcEnabledTableMetadataList) {
            addOrUpdateTableSchema(new KeyspaceTable(tm), new KeyValueSchema(this.kafkaTopicPrefix, tm, this.sourceInfoStructMaker));
        }
        this.cassandraClient.getCluster().register(schemaChangeListener);
        logger().info("Initialized SchemaHolder.");
    }

    public KeyValueSchema getKeyValueSchema(KeyspaceTable kst) {
        return tableToKVSchemaMap.getOrDefault(kst, null);
    }

    public Set<TableMetadata> getCdcEnabledTableMetadataSet() {
        return tableToKVSchemaMap.values().stream()
                .map(KeyValueSchema::tableMetadata)
                .collect(Collectors.toSet());
    }

    protected void removeTableSchema(KeyspaceTable kst) {
        tableToKVSchemaMap.remove(kst);
        logger().info("Removed the schema for {}.{} from table schema cache.", kst.keyspace, kst.table);
    }

    protected void addOrUpdateTableSchema(KeyspaceTable kst, KeyValueSchema kvs) {
        boolean isUpdate = tableToKVSchemaMap.containsKey(kst);
        tableToKVSchemaMap.put(kst, kvs);
        if (isUpdate) {
            logger().info("Updated the schema for {}.{} in table schema cache.", kst.keyspace, kst.table);
        }
        else {
            logger().info("Added the schema for {}.{} to table schema cache.", kst.keyspace, kst.table);
        }
    }
}
