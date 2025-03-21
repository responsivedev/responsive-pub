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
import java.util.Optional;
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
      super(
          tableName,
          client,
          null,
          Optional.empty(),
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null
      );
    }

    @Override
    public BoundStatement insert(
        final int kafkaPartition,
        final Bytes key,
        final byte[] value,
        long timestampMs
    ) {
      return capturingStatement("insertData", new Object[]{name(), kafkaPartition, key, value});
    }

    @Override
    public BoundStatement delete(
        final int kafkaPartition,
        final Bytes key
    ) {
      return capturingStatement("deleteData", new Object[]{name(), kafkaPartition, key});
    }
  }

  private interface ArgumentCapturingStatement extends BoundStatement {

    String callingMethod();

    Object[] args();

  }

}