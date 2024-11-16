/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.internal.db.mongo;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import dev.responsive.kafka.internal.db.RemoteTable;
import dev.responsive.kafka.internal.db.RemoteWriter;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoWriter<K, P, D> implements RemoteWriter<K, P> {

  private static final Logger LOG = LoggerFactory.getLogger(MongoWriter.class);

  private final RemoteTable<K, WriteModel<D>> table;
  private final int kafkaPartition;
  private final P tablePartition;
  private final Supplier<MongoCollection<D>> collection;
  private final List<WriteModel<D>> accumulatedWrites = new ArrayList<>();

  public MongoWriter(
      final RemoteTable<K, WriteModel<D>> table,
      final int kafkaPartition,
      final P tablePartition,
      final Supplier<MongoCollection<D>> collection
  ) {
    this.table = table;
    this.kafkaPartition = kafkaPartition;
    this.tablePartition = tablePartition;
    this.collection = collection;
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

    final BulkWriteOptions options = new BulkWriteOptions().ordered(false);
    try {
      collection.get().bulkWrite(accumulatedWrites, options);
      accumulatedWrites.clear();
      return CompletableFuture.completedFuture(RemoteWriteResult.success(tablePartition));
    } catch (final MongoBulkWriteException e) {
      LOG.error("Failed to flush to {}[{}]. If the exception contains 'E11000 duplicate key', "
                    + "then it was likely this writer was fenced",
                table.name(), tablePartition, e);
      return CompletableFuture.completedFuture(RemoteWriteResult.failure(tablePartition));
    }
  }

}
