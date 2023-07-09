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

package dev.responsive.kafka.store;

import static dev.responsive.db.CassandraClient.UNSET_PERMIT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.internal.core.cql.DefaultBatchStatement;
import dev.responsive.db.CassandraClient;
import dev.responsive.model.Result;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
class AtomicWriterTest {

  private static final UUID PERMIT = UUID.randomUUID();
  private static final long OFFSET = 123L;

  @Mock
  private CassandraClient client;
  @Mock
  private AsyncResultSet asyncResultSet;
  @Captor
  private ArgumentCaptor<Statement<?>> statementCaptor;

  @BeforeEach
  public void beforeEach() {
    when(client.acquirePermit(any(), anyInt(), any(), any(), anyLong()))
        .thenAnswer(iom -> capturingStatement("acquirePermit", iom.getArguments()));
    when(client.revokePermit(any(), anyInt(), anyLong()))
        .thenAnswer(iom -> capturingStatement("revokePermit", iom.getArguments()));
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
  public void shouldWriteOffsetCommitOnEmptyBatch() {
    // Given:
    final var writer = new AtomicWriter<>(
        client,
        "foo",
        0,
        new TestPlugin(),
        OFFSET,
        PERMIT,
        10
    );

    // When:
    writer.flush();

    // Then:
    verify(client, times(1)).executeAsync(statementCaptor.capture());
    final DefaultBatchStatement sent = (DefaultBatchStatement) statementCaptor.getValue();

    final var statements = new ArrayList<ArgumentCapturingStatement>();
    sent.iterator().forEachRemaining(stmt -> statements.add((ArgumentCapturingStatement) stmt));

    assertThat(statements.size(), is(1));
    assertThat(statements.get(0).callingMethod(), is("acquirePermit"));
    assertThat(statements.get(0).args(), is(new Object[]{
        "foo", 0, UNSET_PERMIT, PERMIT, OFFSET
    }));
  }

  @Test
  public void shouldRevokeOffsetCommitOnEmptyBatchNoPermit() {
    // Given:
    final var writer = new AtomicWriter<>(
        client,
        "foo",
        0,
        new TestPlugin(),
        OFFSET,
        null,
        10
    );

    // When:
    writer.flush();

    // Then:
    verify(client, times(1)).executeAsync(statementCaptor.capture());
    final DefaultBatchStatement sent = (DefaultBatchStatement) statementCaptor.getValue();

    final var statements = new ArrayList<ArgumentCapturingStatement>();
    sent.iterator().forEachRemaining(stmt -> statements.add((ArgumentCapturingStatement) stmt));

    assertThat(statements.size(), is(1));
    assertThat(statements.get(0).callingMethod(), is("revokePermit"));
    assertThat(statements.get(0).args(), is(new Object[]{"foo", 0, OFFSET }));
  }

  @Test
  public void shouldFlushInBatches() {
    final var writer = new AtomicWriter<>(
        client,
        "foo",
        0,
        new TestPlugin(),
        OFFSET,
        PERMIT,
        2
    );

    // When:
    writer.addAll(List.of(
        Result.value(Bytes.wrap(new byte[]{0}), new byte[]{0}),
        Result.tombstone(Bytes.wrap(new byte[]{1})),
        Result.value(Bytes.wrap(new byte[]{2}), new byte[]{2})
    ));
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
    assertThat(statements1.get(0).callingMethod(), is("acquirePermit"));
    assertThat(statements1.get(1).callingMethod(), is("insertData"));
    assertThat(statements1.get(2).callingMethod(), is("deleteData"));

    // batch 2:
    final DefaultBatchStatement batch2 = (DefaultBatchStatement) sent.get(1);
    final var statements2 = new ArrayList<ArgumentCapturingStatement>();
    batch2.iterator().forEachRemaining(stmt -> statements2.add((ArgumentCapturingStatement) stmt));

    assertThat(statements2.size(), is(2));
    assertThat(statements2.get(0).callingMethod(), is("acquirePermit"));
    assertThat(statements2.get(0).args(), is(new Object[]{
        "foo", 0, PERMIT, PERMIT, OFFSET
    }));
    assertThat(statements2.get(1).callingMethod(), is("insertData"));
  }

  @Test
  public void shouldNotIssueSecondBatchIfFirstWasNotApplied() {
    when(asyncResultSet.wasApplied()).thenReturn(false);
    final var writer = new AtomicWriter<>(
        client,
        "foo",
        0,
        new TestPlugin(),
        OFFSET,
        PERMIT,
        2
    );

    // When:
    writer.addAll(List.of(
        Result.value(Bytes.wrap(new byte[]{0}), new byte[]{0}),
        Result.tombstone(Bytes.wrap(new byte[]{1})),
        Result.value(Bytes.wrap(new byte[]{2}), new byte[]{2})
    ));
    writer.flush();

    // Then:
    verify(client, times(1)).executeAsync(statementCaptor.capture());
  }

  private static class TestPlugin implements BufferPlugin<Bytes> {

    @Override
    public Bytes keyFromRecord(final ConsumerRecord<byte[], byte[]> record) {
      return Bytes.wrap(record.key());
    }

    @Override
    public BoundStatement insertData(
        final CassandraClient client,
        final String tableName,
        final int partition,
        final Bytes key,
        final byte[] value
    ) {
      return capturingStatement("insertData", new Object[]{tableName, partition, key, value});
    }

    @Override
    public BoundStatement deleteData(
        final CassandraClient client,
        final String tableName,
        final int partition,
        final Bytes key
    ) {
      return capturingStatement("deleteData", new Object[]{tableName, partition, key});
    }

    @Override
    public Bytes bytes(final Bytes key) {
      return key;
    }

    @Override
    public int compare(final Bytes o1, final Bytes o2) {
      return o1.compareTo(o2);
    }
  }

  private interface ArgumentCapturingStatement extends BoundStatement {

    String callingMethod();

    Object[] args();

  }

}