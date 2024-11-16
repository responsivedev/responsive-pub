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

package dev.responsive.kafka.internal.db;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import java.util.Map;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CassandraClientTest {

  @Mock
  private CqlSession session;
  @Captor
  private ArgumentCaptor<Statement<?>> statementCaptor;

  @Test
  public void allStatementsShouldBeIdempotent() {
    // Given:
    final CassandraClient client = new CassandraClient(
        session,
        ResponsiveConfig.loggedConfig(Map.of(
            ResponsiveConfig.RESPONSIVE_ORG_CONFIG, "ignored",
            ResponsiveConfig.RESPONSIVE_ENV_CONFIG, "ignored",
            ResponsiveConfig.CASSANDRA_HOSTNAME_CONFIG, "ignored",
            ResponsiveConfig.CASSANDRA_PORT_CONFIG, 0
        ))
    );
    when(session.execute(statementCaptor.capture())).thenReturn(null);

    // When:
    client.execute(SimpleStatement.newInstance("INSERT INTO foo (id) VALUES (1);"));

    // Then:
    final Statement<?> value = statementCaptor.getValue();
    assertThat(value.isIdempotent(), Matchers.is(true));
  }

  @Test
  public void shouldDefaultReadConsistencyToQuorum() {
    // Given:
    final CassandraClient client = new CassandraClient(
        session,
        ResponsiveConfig.loggedConfig(Map.of(
            ResponsiveConfig.RESPONSIVE_ORG_CONFIG, "ignored",
            ResponsiveConfig.RESPONSIVE_ENV_CONFIG, "ignored",
            ResponsiveConfig.CASSANDRA_HOSTNAME_CONFIG, "ignored",
            ResponsiveConfig.CASSANDRA_PORT_CONFIG, 0
        ))
    );
    when(session.prepare((SimpleStatement) statementCaptor.capture())).thenReturn(null);

    // When:
    client.prepare(SimpleStatement.newInstance("SELECT * FROM foo;"), QueryOp.READ);

    // Then:
    final Statement<?> value = statementCaptor.getValue();
    assertThat(value.getConsistencyLevel(), Matchers.is(ConsistencyLevel.QUORUM));
  }

  @Test
  public void shouldDefaultWriteConsistencyToQuorum() {
    // Given:
    final CassandraClient client = new CassandraClient(
        session,
        ResponsiveConfig.loggedConfig(Map.of(
            ResponsiveConfig.RESPONSIVE_ORG_CONFIG, "ignored",
            ResponsiveConfig.RESPONSIVE_ENV_CONFIG, "ignored",
            ResponsiveConfig.CASSANDRA_HOSTNAME_CONFIG, "ignored",
            ResponsiveConfig.CASSANDRA_PORT_CONFIG, 0
        ))
    );
    when(session.prepare((SimpleStatement) statementCaptor.capture())).thenReturn(null);

    // When:
    client.prepare(SimpleStatement.newInstance("INSERT INTO foo (id) VALUES (1);"), QueryOp.WRITE);

    // Then:
    final Statement<?> value = statementCaptor.getValue();
    assertThat(value.getConsistencyLevel(), Matchers.is(ConsistencyLevel.QUORUM));
  }

  @Test
  public void shouldOverwriteConsistencyLevelsIfConfigIsSetForRead() {
    // Given:
    final CassandraClient client = new CassandraClient(
        session,
        ResponsiveConfig.loggedConfig(Map.of(
            ResponsiveConfig.RESPONSIVE_ORG_CONFIG, "ignored",
            ResponsiveConfig.RESPONSIVE_ENV_CONFIG, "ignored",
            ResponsiveConfig.CASSANDRA_HOSTNAME_CONFIG, "ignored",
            ResponsiveConfig.CASSANDRA_PORT_CONFIG, 0,
            ResponsiveConfig.READ_CONSISTENCY_LEVEL_CONFIG, "ALL"
        ))
    );
    when(session.prepare((SimpleStatement) statementCaptor.capture())).thenReturn(null);

    // When:
    client.prepare(SimpleStatement.newInstance("SELECT * FROM foo;"), QueryOp.READ);

    // Then:
    final Statement<?> value = statementCaptor.getValue();
    assertThat(value.getConsistencyLevel(), Matchers.is(ConsistencyLevel.ALL));
  }

  @Test
  public void shouldOverwriteConsistencyLevelsIfConfigIsSetForWrite() {
    // Given:
    final CassandraClient client = new CassandraClient(
        session,
        ResponsiveConfig.loggedConfig(Map.of(
            ResponsiveConfig.RESPONSIVE_ORG_CONFIG, "ignored",
            ResponsiveConfig.RESPONSIVE_ENV_CONFIG, "ignored",
            ResponsiveConfig.CASSANDRA_HOSTNAME_CONFIG, "ignored",
            ResponsiveConfig.CASSANDRA_PORT_CONFIG, 0,
            ResponsiveConfig.WRITE_CONSISTENCY_LEVEL_CONFIG, "ALL"
        ))
    );
    when(session.prepare((SimpleStatement) statementCaptor.capture())).thenReturn(null);

    // When:
    client.prepare(SimpleStatement.newInstance("INSERT INTO foo (id) VALUES (1);"), QueryOp.WRITE);

    // Then:
    final Statement<?> value = statementCaptor.getValue();
    assertThat(value.getConsistencyLevel(), Matchers.is(ConsistencyLevel.ALL));
  }

}