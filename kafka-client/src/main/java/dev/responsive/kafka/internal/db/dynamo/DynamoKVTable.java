/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.db.dynamo;

import static dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration.NO_COMMITTED_OFFSET;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.responsive.kafka.internal.db.KVFlushManager;
import dev.responsive.kafka.internal.db.RemoteKVTable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class DynamoKVTable implements RemoteKVTable<DynamoDbRequest> {

  private static final Logger LOG = LoggerFactory.getLogger(DynamoKVTable.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String name;
  private final DynamoDbAsyncClient dynamoDB;
  private final ConcurrentMap<Integer, Long> kafkaPartitionToEpoch = new ConcurrentHashMap<>();

  public DynamoKVTable(
      final DynamoDbAsyncClient dynamoDB,
      final String name
  ) {
    this.dynamoDB = dynamoDB;
    this.name = name;
    createTable(name, ScalarAttributeType.B);
    createTable(metaTableName(name), ScalarAttributeType.N);
  }

  private void createTable(final String name, final ScalarAttributeType keyType) {
    final var createTable = CreateTableRequest.builder()
        .tableName(name)
        .keySchema(KeySchemaElement.builder()
            .attributeName("key")
            .keyType(KeyType.HASH)
            .build())
        .attributeDefinitions(AttributeDefinition.builder()
            .attributeName("key")
            .attributeType(keyType)
            .build())
        .billingMode(BillingMode.PAY_PER_REQUEST) // TODO(agavra): allow provisioned throughput
        .build();

    try {
      dynamoDB
          .createTable(createTable)
          .thenApply(resp -> resp.tableDescription().tableName())
          .exceptionally(e -> {
            if (e.getCause() instanceof ResourceInUseException) {
              // ignored
              return name;
            } else {
              throw new RuntimeException(e);
            }
          })
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public KVFlushManager init(final int kafkaPartition) {
    try {
      final UpdateItemRequest update = UpdateItemRequest.builder()
          .tableName(metaTableName(this.name))
          .key(Map.of(
              "key", AttributeValue.fromN(String.valueOf(kafkaPartition))
          ))
          .updateExpression(
              "SET epoch = if_not_exists(epoch, :zero) + :one, #o = if_not_exists(#o, :offsetValue)"
          )
          .expressionAttributeNames(Map.of(
              "#o", "offset"
          ))
          .expressionAttributeValues(Map.of(
              ":zero", AttributeValue.fromN("0"),
              ":one", AttributeValue.fromN("1"),
              ":offsetValue", AttributeValue.fromN(String.valueOf(NO_COMMITTED_OFFSET))
          ))
          .returnValues(ReturnValue.UPDATED_NEW)
          .build();

      final var result = dynamoDB.updateItem(update).get();
      final var epoch = result.attributes().get("epoch").n();
      kafkaPartitionToEpoch.put(kafkaPartition, Long.parseLong(epoch));
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }

    return new DynamoKVFlushManager(this, kafkaPartition, dynamoDB);
  }

  @Override
  public byte[] get(final int kafkaPartition, final Bytes key, final long streamTimeMs) {
    // TODO(agavra): support minValidTs
    final GetItemRequest req = GetItemRequest.builder()
        .tableName(name)
        .key(Map.of("key", AttributeValue.fromB(SdkBytes.fromByteArray(key.get()))))
        .build();

    try {
      final GetItemResponse resp = dynamoDB.getItem(req).get();
      return resp.hasItem() ? resp.item().get("val").b().asByteArray() : null;
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(
      final int kafkaPartition,
      final Bytes from,
      final Bytes to,
      final long streamTimeMs
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all(final int kafkaPartition, final long streamTimeMs) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long approximateNumEntries(final int kafkaPartition) {
    LOG.warn("approximateNumEntries is not yet implemented for DynamoDB");
    return 0;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public PutItemRequest insert(
      final int kafkaPartition,
      final Bytes key,
      final byte[] value,
      final long epochMillis
  ) {
    final long epoch = kafkaPartitionToEpoch.get(kafkaPartition);
    // TODO(agavra): account for TTL index and tombstones once delete is updated
    return PutItemRequest.builder()
        .tableName(name)
        .item(Map.of(
            "key", AttributeValue.fromB(SdkBytes.fromByteArray(key.get())),
            "val", AttributeValue.fromB(SdkBytes.fromByteArray(value)),
            "epoch", AttributeValue.fromN(String.valueOf(epoch)),
            "ts", AttributeValue.fromN(String.valueOf(epochMillis))
        ))
        .conditionExpression("attribute_not_exists(epoch) OR epoch <= :epochValue")
        .expressionAttributeValues(Map.of(
            ":epochValue", AttributeValue.fromN(String.valueOf(epoch))
        ))
        .build();
  }

  @Override
  public DeleteItemRequest delete(final int kafkaPartition, final Bytes key) {
    // TODO(agavra): replace me with tombstone and ttl index
    final long epoch = kafkaPartitionToEpoch.get(kafkaPartition);
    return DeleteItemRequest.builder()
        .tableName(name)
        .key(Map.of("key", AttributeValue.fromB(SdkBytes.fromByteArray(key.get()))))
        .conditionExpression("attribute_not_exists(epoch) OR epoch <= :epochValue")
        .expressionAttributeValues(Map.of(
            ":epochValue", AttributeValue.fromN(String.valueOf(epoch))
        ))
        .build();
  }

  public void setOffset(final int kafkaPartition, final long offset) {
    final long epoch = kafkaPartitionToEpoch.get(kafkaPartition);

    try {
      final var req = PutItemRequest.builder()
          .tableName(metaTableName(name))
          .item(Map.of(
              "key", AttributeValue.fromN(String.valueOf(kafkaPartition)),
              "epoch", AttributeValue.fromN(String.valueOf(epoch)),
              "offset", AttributeValue.fromN(String.valueOf(offset))
          ))
          .conditionExpression("attribute_not_exists(epoch) OR epoch <= :epochValue")
          .expressionAttributeValues(Map.of(
              ":epochValue", AttributeValue.fromN(String.valueOf(epoch))
          ))
          .build();

      // TODO(agavra): should we check if it worked (re: epoch)?
      dynamoDB.putItem(req).get();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long fetchOffset(final int kafkaPartition) {
    final var offset = getMeta(kafkaPartition).get("offset").n();
    return Long.parseLong(offset);
  }

  private static String metaTableName(final String tableName) {
    return tableName + "_md";
  }

  public long localEpoch(final int kafkaPartition) {
    return kafkaPartitionToEpoch.get(kafkaPartition);
  }

  public long fetchEpoch(final int kafkaPartition) {
    final var epoch = getMeta(kafkaPartition).get("epoch").n();
    return Long.parseLong(epoch);
  }

  private Map<String, AttributeValue> getMeta(final int kafkaPartition) {
    final var req = GetItemRequest.builder()
        .tableName(metaTableName(name))
        .key(Map.of("key", AttributeValue.fromN(String.valueOf(kafkaPartition))))
        .build();
    try {
      final var resp = dynamoDB.getItem(req).get();
      if (resp.hasItem()) {
        return resp.item();
      }
      throw new IllegalStateException(
          "Could not find metadata row for partition " + kafkaPartition + " in table "
              + metaTableName(name));
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
