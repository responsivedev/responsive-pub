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