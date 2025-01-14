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

package dev.responsive.kafka.internal.db;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public class FactSchemaWriter<K> implements RemoteWriter<K, Integer> {

  private final CassandraClient client;
  private final RemoteTable<K, BoundStatement> table;
  private final int kafkaPartition;
  private final int tablePartition;

  private final List<BoundStatement> statements;

  public FactSchemaWriter(
      final CassandraClient client,
      final RemoteTable<K, BoundStatement> table,
      final int kafkaPartition,
      final int tablePartition
  ) {
    this.client = client;
    this.table = table;
    this.kafkaPartition = kafkaPartition;
    this.tablePartition = tablePartition;

    statements = new ArrayList<>();
  }

  @Override
  public void insert(final K key, final byte[] value, long timestampMs) {
    statements.add(table.insert(kafkaPartition, key, value, timestampMs));
  }

  @Override
  public void delete(final K key) {
    statements.add(table.delete(kafkaPartition, key));
  }

  @Override
  public CompletionStage<RemoteWriteResult<Integer>> flush() {
    final List<CompletionStage<RemoteWriteResult<Integer>>> results = statements.stream()
        .map(this::executeAsync)
        .collect(Collectors.toList());

    // if any of them failed, the result should be a failure, otherwise they will
    // all have the same result anyway, so we can return just a success(partition)
    //
    // it's safe to run these in parallel w/o a guaranteed ordering because commit
    // buffer only maintains the latest value per key, and we only flush that, also
    // fact schema expects immutable values
    var result = CompletableFuture.completedStage(RemoteWriteResult.success(tablePartition));
    for (final CompletionStage<RemoteWriteResult<Integer>> future : results) {
      result = result.thenCombine(future, (one, two) -> !one.wasApplied() ? one : two);
    }

    return result;
  }

  private CompletionStage<RemoteWriteResult<Integer>> executeAsync(final BoundStatement statement) {
    return client.executeAsync(statement)
        .thenApply(resp -> RemoteWriteResult.of(tablePartition, resp));
  }
}
