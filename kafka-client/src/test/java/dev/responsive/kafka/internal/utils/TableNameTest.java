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

package dev.responsive.kafka.internal.utils;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

class TableNameTest {

  @Test
  public void shouldReplaceInvalidCharsWithUnderscores() {
    // Given:
    final var kafkaName = "foo.Bar-baz_qux";

    // When:
    final var name = new TableName(kafkaName);

    // Then:
    MatcherAssert.assertThat(name.tableName(), Matchers.is("foo_bar_baz__qux"));
  }

}