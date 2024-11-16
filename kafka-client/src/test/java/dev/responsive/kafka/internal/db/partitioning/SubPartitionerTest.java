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

package dev.responsive.kafka.internal.db.partitioning;

import static dev.responsive.kafka.api.config.ResponsiveConfig.CASSANDRA_DESIRED_NUM_PARTITION_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.SUBPARTITION_HASHER_CONFIG;
import static dev.responsive.kafka.testutils.IntegrationTestUtils.dummyConfig;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.utils.TableName;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Bytes;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SubPartitionerTest {

  private static final TableName NAME = new TableName("table");
  private static final String CHANGELOG_TOPIC_NAME = "changelog";

  @Test
  public void shouldMapPartitionsToLargerSpace() {
    // Given:
    final var partitioner = new SubPartitioner(2, new SingleByteHasher());

    // When:
    final int zero = partitioner.tablePartition(0, Bytes.wrap(new byte[]{0}));
    final int one = partitioner.tablePartition(0, Bytes.wrap(new byte[]{1}));
    final int two = partitioner.tablePartition(1, Bytes.wrap(new byte[]{2}));
    final int three = partitioner.tablePartition(1, Bytes.wrap(new byte[]{3}));

    // Then:
    assertThat(zero, is(0));
    assertThat(one, is(1));
    assertThat(two, is(2));
    assertThat(three, is(3));
  }

  @Test
  public void shouldIterateAllSubPartitionsInOrder() {
    // Given:
    final var partitioner = new SubPartitioner(3, new SingleByteHasher());

    // When:
    final List<Integer> result = partitioner.allTablePartitions(2);

    // Then:
    assertThat(result, contains(6, 7, 8));
  }

  @Test
  public void shouldConfigureSubPartitionerWhenDesireIsNotMultipleOfActual() {
    // Given:
    final var actualRemoteCount = OptionalInt.of(128);
    final int kafkaPartitions = 32;
    final int desiredPartitions = 127;

    // When:
    final SubPartitioner subPartitioner = SubPartitioner.create(
        actualRemoteCount,
        kafkaPartitions,
        NAME.tableName(),
        responsiveConfig(desiredPartitions),
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
        NAME.tableName(),
        responsiveConfig(desiredPartitions),
        CHANGELOG_TOPIC_NAME
    );

    // Then:
    assertThat(subPartitioner.getFactor(), is(1));
  }

  @Test
  public void shouldThrowExceptionIfActualPartitionsDoesNotMatchComputedPartitions() {
    // Given:
    final var actualRemoteCount = OptionalInt.of(100);
    final int kafkaPartitions = 32;
    final int desiredPartitions = 127;

    // Expect:
    final ConfigException error = Assertions.assertThrows(
        ConfigException.class,
        () -> SubPartitioner.create(
            actualRemoteCount,
            kafkaPartitions,
            NAME.tableName(),
            responsiveConfig(desiredPartitions),
            CHANGELOG_TOPIC_NAME
        )
    );

    // Then:
    assertThat(
        error.getMessage(),
        Matchers.allOf(
            containsString("was configured to 127, which given 32 partitions in "
            + "kafka topic changelog would result in 128 remote partitions"),
            containsString("already initialized with 100 partitions")
        )
    );
  }

  public static class SingleByteHasher implements Hasher {

    @Override
    public Integer apply(final Bytes bytes) {
      return (int) bytes.get()[0];
    }
  }

  private ResponsiveConfig responsiveConfig(final int desiredNumSubPartitions) {
    return dummyConfig(Map.of(
        CASSANDRA_DESIRED_NUM_PARTITION_CONFIG, desiredNumSubPartitions,
        SUBPARTITION_HASHER_CONFIG, SingleByteHasher.class
    ));
  }

}