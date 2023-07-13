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

package dev.responsive.db;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
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
    final CassandraClient client = new CassandraClient(session);
    when(session.execute(statementCaptor.capture())).thenReturn(null);

    // When:
    client.execute(SimpleStatement.newInstance("INSERT INTO foo (id) VALUES (1);"));

    // Then:
    final Statement<?> value = statementCaptor.getValue();
    assertThat(value.isIdempotent(), Matchers.is(true));
  }

}