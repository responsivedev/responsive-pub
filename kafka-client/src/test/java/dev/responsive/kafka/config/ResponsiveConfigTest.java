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

package dev.responsive.kafka.config;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import dev.responsive.utils.SubPartitioner;
import dev.responsive.utils.TableName;
import java.util.OptionalInt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ResponsiveConfigTest {

  private static final TableName NAME = new TableName("table");
  private static final String CHANGELOG_TOPIC_NAME = "changelog";

  @Test
  public void shouldConfigureSubPartitionerWhenDesireIsNotMultipleOfActual() {
    // Given:
    final var actualRemoteCount = OptionalInt.of(128);
    final int kafkaPartitions = 32;
    final int desiredPartitions = 129;

    // When:
    final SubPartitioner subPartitioner = ResponsiveConfig.getSubPartitioner(
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
    final SubPartitioner subPartitioner = ResponsiveConfig.getSubPartitioner(
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
    final IllegalArgumentException error = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ResponsiveConfig.getSubPartitioner(
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