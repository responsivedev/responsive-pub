/*
 *
 *  * Copyright 2023 Responsive Computing, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package dev.responsive.kafka.internal.db;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.FactSchemaWriter;
import dev.responsive.kafka.internal.db.RemoteTable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.utils.Bytes;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FactSchemaWriterTest {

  private static final long CURRENT_TS = 100L;

  @Mock private CassandraClient client;
  @Mock private RemoteTable<Bytes, BoundStatement> table;
  @Mock private AsyncResultSet result;

  private final ArgumentCaptor<Statement<?>> statementCaptor =
      ArgumentCaptor.forClass(Statement.class);

  @Test
  public void shouldNotUseBatchesOnFlush() {
    // Given:
    when(table.insert(anyInt(), any(), any(), anyLong()))
        .thenReturn(mock(BoundStatement.class));
    when(result.wasApplied()).thenReturn(true);
    when(client.executeAsync(statementCaptor.capture()))
        .thenReturn(CompletableFuture.completedFuture(result));

    // When:
    final FactSchemaWriter<Bytes> writer = new FactSchemaWriter<>(
        client,
        table,
        0
    );
    writer.insert(Bytes.wrap(new byte[]{0}), new byte[]{1}, CURRENT_TS);
    writer.flush();

    // Then:
    MatcherAssert.assertThat(
        statementCaptor.getValue(),
        not(instanceOf(BatchStatement.class))
    );
  }

  @Test
  @Timeout(5)
  public void shouldIssueInsertsInParallel() throws ExecutionException, InterruptedException {
    // Given:
    when(table.insert(anyInt(), any(), any(), anyLong()))
        .thenReturn(mock(BoundStatement.class));
    when(result.wasApplied()).thenReturn(true);

    final var latch = new CountDownLatch(1);
    when(client.executeAsync(statementCaptor.capture()))
        .thenAnswer(iom -> CompletableFuture.supplyAsync(() -> {
          try {
            latch.await();
            return result;
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }))
        .thenAnswer(iom -> CompletableFuture.supplyAsync(() -> {
          latch.countDown();
          return result;
        }));

    // When:
    final FactSchemaWriter<Bytes> writer = new FactSchemaWriter<>(
        client,
        table,
        0
    );

    writer.insert(Bytes.wrap(new byte[]{0}), new byte[]{1}, CURRENT_TS);
    writer.insert(Bytes.wrap(new byte[]{0}), new byte[]{1}, CURRENT_TS);
    writer.flush().toCompletableFuture().get();

    // Then:
    MatcherAssert.assertThat(
        statementCaptor.getValue(),
        not(instanceOf(BatchStatement.class))
    );
  }

}