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

package dev.responsive.internal.structs;

import static org.hamcrest.MatcherAssert.assertThat;

import dev.responsive.internal.db.BytesKeySpec;
import dev.responsive.internal.utils.Result;
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