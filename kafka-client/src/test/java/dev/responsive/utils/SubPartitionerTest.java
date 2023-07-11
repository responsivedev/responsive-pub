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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SubPartitionerTest {

  private static final TableName NAME = new TableName("table");
  private static final String CHANGELOG_TOPIC_NAME = "changelog";

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

  @Test
  public void shouldConfigureSubPartitionerWhenDesireIsNotMultipleOfActual() {
    // Given:
    final var actualRemoteCount = OptionalInt.of(128);
    final int kafkaPartitions = 32;
    final int desiredPartitions = 129;

    // When:
    final SubPartitioner subPartitioner = SubPartitioner.create(
        actualRemoteCount,
        kafkaPartitions,
        desiredPartitions,
        NAME,
        CHANGELOG_TOPIC_NAME
    );

    // Then:
    assertThat(subPartitioner.getFactor(), is(4));
  }

  @Test
  public void shouldConfigureSubPartitionerWhenDesiredIsNegativeOne() {
    // Given:
    final var actualRemoteCount = OptionalInt.of(32);
    final int kafkaPartitions = 32;
    final int desiredPartitions = -1;

    // When:
    final SubPartitioner subPartitioner = SubPartitioner.create(
        actualRemoteCount,
        kafkaPartitions,
        desiredPartitions,
        NAME,
        CHANGELOG_TOPIC_NAME
    );

    // Then:
    assertThat(subPartitioner.getFactor(), is(1));
  }

  @Test
  public void shouldThrowExceptionIfActualPartitionsDoesNotMatchComputedPartitions() {
    // Given:
    final var actualRemoteCount = OptionalInt.of(129);
    final int kafkaPartitions = 32;
    final int desiredPartitions = 129;

    // Expect:
    final ConfigException error = Assertions.assertThrows(
        ConfigException.class,
        () -> SubPartitioner.create(
            actualRemoteCount,
            kafkaPartitions,
            desiredPartitions,
            NAME,
            CHANGELOG_TOPIC_NAME
        )
    );

    // Then:
    assertThat(
        error.getMessage(),
        containsString("was configured to 129, which given 32 partitions in "
            + "kafka topic changelog would result in 128 remote partitions"));
  }

}