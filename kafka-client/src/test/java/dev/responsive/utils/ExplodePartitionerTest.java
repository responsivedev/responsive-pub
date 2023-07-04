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

package dev.responsive.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.kafka.streams.processor.StreamPartitioner;
import org.junit.jupiter.api.Test;

class ExplodePartitionerTest {

  @Test
  public void shouldMapPartitionsToLargerSpace() {
    // Given:
    final StreamPartitioner<Integer, Integer> base = (t, k, v, np) -> k % np;
    final ExplodePartitioner<Integer, Integer> partitioner = new ExplodePartitioner<>(
        base,
        "",
        2,
        1
    );

    // When:
    final int zero = partitioner.repartition(0, 0);
    final int one = partitioner.repartition(1, 0);
    final int two = partitioner.repartition(2, 0);
    final int three = partitioner.repartition(3, 0);

    // Then:
    assertThat(zero, is(0));
    assertThat(one, is(1));
    assertThat(two, is(0));
    assertThat(three, is(1));
  }

}