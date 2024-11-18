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

import static org.hamcrest.MatcherAssert.assertThat;

import dev.responsive.kafka.internal.db.BytesKeySpec;
import dev.responsive.kafka.internal.utils.Result;
import org.apache.kafka.common.utils.Bytes;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

class ResultTest {

  @Test
  public void shouldComputeSize() {
    // Given:
    final Result<Bytes> result = Result.value(Bytes.wrap(new byte[10]), new byte[10], 1L);

    // When:
    final int size = result.size(new BytesKeySpec());

    // Then:
    // 10 for key, 10 for value, 8 for timestamp
    assertThat(size, Matchers.is(28));
  }

  @Test
  public void shouldComputeSizeForTombstone() {
    // Given:
    final Result<Bytes> result = Result.tombstone(Bytes.wrap(new byte[10]), 1L);

    // When:
    final int size = result.size(new BytesKeySpec());

    // Then:
    // 10 for key, 8 for timestamp
    assertThat(size, Matchers.is(18));
  }

}