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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Test;

class SubPartitionerTest {

  @Test
  public void shouldMapPartitionsToLargerSpace() {
    // Given:
    final var partitioner = new SubPartitioner(2, k -> (int) k.get()[0]);

    // When:
    final int zero = partitioner.partition(0, Bytes.wrap(new byte[]{0}));
    final int one = partitioner.partition(0, Bytes.wrap(new byte[]{1}));
    final int two = partitioner.partition(1, Bytes.wrap(new byte[]{2}));
    final int three = partitioner.partition(1, Bytes.wrap(new byte[]{3}));

    // Then:
    assertThat(zero, is(0));
    assertThat(one, is(1));
    assertThat(two, is(2));
    assertThat(three, is(3));
  }

  @Test
  public void shouldIterateAllSubPartitions() {
    // Given:
    final var partitioner = new SubPartitioner(2, k -> (int) k.get()[0]);

    // When:
    final List<Integer> result = partitioner.all(2).boxed().collect(Collectors.toList());

    // Then:
    assertThat(result, contains(4, 5));
  }

  @Test
  public void shouldNotChangeSalt() {
    assertThat(
        "changing the salt from 31 to another number is backwards incompatible!",
        SubPartitioner.SALT,
        is(31)
    );
  }

}