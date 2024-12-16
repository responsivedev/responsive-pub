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

import com.google.common.collect.Iterators;
import dev.responsive.kafka.internal.db.RemoteTable;
import dev.responsive.kafka.internal.db.RemoteWriter;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

public class DynamoWriter<K, P> implements RemoteWriter<K, P> {

  private static final Logger LOG = LoggerFactory.getLogger(DynamoWriter.class);
  private final RemoteTable<K, DynamoDbRequest>
      table;
  private final int kafkaPartition;
  private final P tablePartition;

  private final List<DynamoDbRequest> accumulatedWrites = new ArrayList<>();
  private final DynamoDbAsyncClient dynamo;

  public DynamoWriter(
      final RemoteTable<K, DynamoDbRequest> table,
      final int kafkaPartition,
      final P tablePartition,
      final DynamoDbAsyncClient dynamo
  ) {
    this.table = table;
    this.kafkaPartition = kafkaPartition;
    this.tablePartition = tablePartition;
    this.dynamo = dynamo;
  }

  @Override
  public void insert(final K key, final byte[] value, final long epochMillis) {
    accumulatedWrites.add(table.insert(kafkaPartition, key, value, epochMillis));
  }

  @Override
  public void delete(final K key) {
    accumulatedWrites.add(table.delete(kafkaPartition, key));
  }

  @Override
  public CompletionStage<RemoteWriteResult<P>> flush() {
    if (accumulatedWrites.isEmpty()) {
      LOG.info("Skipping empty bulk write for partition {}", tablePartition);
      return CompletableFuture.completedFuture(RemoteWriteResult.success(tablePartition));
    }

    try {
      final List<CompletableFuture<?>> futures = new ArrayList<>();
      final var requests = Iterators.consumingIterator(accumulatedWrites.iterator());
      while (requests.hasNext()) {
        final var request = requests.next();
        if (request instanceof PutItemRequest) {
          futures.add(dynamo.putItem((PutItemRequest) request));
        } else if (request instanceof DeleteItemRequest) {
          futures.add(dynamo.deleteItem((DeleteItemRequest) request));
        } else {
          throw new IllegalArgumentException("Unexpected accumulated request: " + request);
        }
      }
      CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
      return CompletableFuture.completedFuture(RemoteWriteResult.success(tablePartition));
    } catch (final Exception e) {
      LOG.error("Failed to flush to {}[{}].", table.name(), tablePartition, e);
      return CompletableFuture.completedFuture(RemoteWriteResult.failure(tablePartition));
    }
  }
}
