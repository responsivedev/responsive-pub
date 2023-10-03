/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.responsive.kafka.internal.db;

import com.datastax.oss.driver.api.core.cql.Statement;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public class FactSchemaWriter<K> implements RemoteWriter<K> {

  private final CassandraClient client;
  private final RemoteSchema<K> schema;
  private final String name;
  private final int partition;

  private final List<Statement<?>> statements;

  public FactSchemaWriter(
      final CassandraClient client,
      final RemoteSchema<K> schema,
      final String name,
      final int partition
  ) {
    this.client = client;
    this.schema = schema;
    this.name = name;
    this.partition = partition;

    statements = new ArrayList<>();
  }

  @Override
  public void insert(final K key, final byte[] value, long epochMillis) {
    statements.add(schema.insert(name, partition, key, value, epochMillis));
  }

  @Override
  public void delete(final K key) {
    statements.add(schema.delete(name, partition, key));
  }

  @Override
  public CompletionStage<RemoteWriteResult> flush() {
    final List<CompletionStage<RemoteWriteResult>> results = statements.stream()
        .map(this::executeAsync)
        .collect(Collectors.toList());

    // if any of them failed, the result should be a failure, otherwise they will
    // all have the same result anyway, so we can return just a success(partition)
    //
    // it's safe to run these in parallel w/o a guaranteed ordering because commit
    // buffer only maintains the latest value per key, and we only flush that, also
    // fact schema expects immutable values
    var result = CompletableFuture.completedStage(RemoteWriteResult.success(partition));
    for (final CompletionStage<RemoteWriteResult> future : results) {
      result = result.thenCombine(future, (one, two) -> !one.wasApplied() ? one : two);
    }

    return result;
  }

  @Override
  public RemoteWriteResult setOffset(final long offset) {
    final var result = client.execute(schema.setOffset(name, partition, offset));
    return result.wasApplied()
        ? RemoteWriteResult.success(partition)
        : RemoteWriteResult.failure(partition);
  }

  private CompletionStage<RemoteWriteResult> executeAsync(final Statement<?> statement) {
    return client.executeAsync(statement)
        .thenApply(resp -> RemoteWriteResult.of(partition, resp));
  }
}
