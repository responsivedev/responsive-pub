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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.internal.core.cql.DefaultBatchStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class LwtWriterTest {

  private static final long CURRENT_TS = 100L;
  
  @Mock
  private CassandraClient client;
  @Mock
  private AsyncResultSet asyncResultSet;
  @Captor
  private ArgumentCaptor<Statement<?>> statementCaptor;

  @BeforeEach
  public void beforeEach() {
    when(client.executeAsync(any()))
        .thenReturn(CompletableFuture.completedFuture(asyncResultSet));
    when(asyncResultSet.wasApplied()).thenReturn(true);
  }

  private static ArgumentCapturingStatement capturingStatement(
      final String method,
      final Object[] args
  ) {
    final var mock = mock(ArgumentCapturingStatement.class);
    when(mock.callingMethod()).thenReturn(method);
    when(mock.args()).thenReturn(args);
    return mock;
  }

  @Test
  public void shouldFlushInBatches() {
    final var writer = new LwtWriter<>(
        client,
        () -> capturingStatement("fencingStatement", new Object[]{}),
        new TestRemoteTable("foo", client),
        0,
        2
    );

    // When:
    writer.insert(Bytes.wrap(new byte[]{0}), new byte[]{0}, CURRENT_TS);
    writer.delete(Bytes.wrap(new byte[]{1}));
    writer.insert(Bytes.wrap(new byte[]{2}), new byte[]{2}, CURRENT_TS);
    writer.flush();

    // Then:
    verify(client, times(2)).executeAsync(statementCaptor.capture());
    final List<Statement<?>> sent = statementCaptor.getAllValues();
    assertThat(sent.size(), is(2));

    // batch 1:
    final DefaultBatchStatement batch1 = (DefaultBatchStatement) sent.get(0);
    final var statements1 = new ArrayList<ArgumentCapturingStatement>();
    batch1.iterator().forEachRemaining(stmt -> statements1.add((ArgumentCapturingStatement) stmt));

    assertThat(statements1.size(), is(3));
    assertThat(statements1.get(0).callingMethod(), is("fencingStatement"));
    assertThat(statements1.get(1).callingMethod(), is("insertData"));
    assertThat(statements1.get(2).callingMethod(), is("deleteData"));

    // batch 2:
    final DefaultBatchStatement batch2 = (DefaultBatchStatement) sent.get(1);
    final var statements2 = new ArrayList<ArgumentCapturingStatement>();
    batch2.iterator().forEachRemaining(stmt -> statements2.add((ArgumentCapturingStatement) stmt));

    assertThat(statements2.size(), is(2));
    assertThat(statements2.get(0).callingMethod(), is("fencingStatement"));
    assertThat(statements2.get(1).callingMethod(), is("insertData"));
  }

  @Test
  public void shouldIssueSecondBatchEvenIfFirstWasNotApplied() {
    when(asyncResultSet.wasApplied()).thenReturn(false);
    final var writer = new LwtWriter<>(
        client,
        () -> capturingStatement("fencingStatement", new Object[]{}),
        new TestRemoteTable("foo", client),
        0,
        2
    );

    // When:
    writer.insert(Bytes.wrap(new byte[]{0}), new byte[]{0}, CURRENT_TS);
    writer.delete(Bytes.wrap(new byte[]{1}));
    writer.insert(Bytes.wrap(new byte[]{2}), new byte[]{2}, CURRENT_TS);
    writer.flush();

    // Then:
    verify(client, times(2)).executeAsync(statementCaptor.capture());
  }

  private static class TestRemoteTable extends CassandraKeyValueTable {

    public TestRemoteTable(final String tableName, final CassandraClient client) {
      super(tableName, client, null, null, null, null, null, null);
    }

    @Override
    public BoundStatement insert(
        final int partition,
        final Bytes key,
        final byte[] value,
        long epochMillis) {
      return capturingStatement("insertData", new Object[]{name(), partition, key, value});
    }

    @Override
    public BoundStatement delete(
        final int partition,
        final Bytes key
    ) {
      return capturingStatement("deleteData", new Object[]{name(), partition, key});
    }
  }

  private interface ArgumentCapturingStatement extends BoundStatement {

    String callingMethod();

    Object[] args();

  }

}