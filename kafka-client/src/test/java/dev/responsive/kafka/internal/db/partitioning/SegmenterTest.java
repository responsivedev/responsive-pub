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

package dev.responsive.kafka.internal.db.partitioning;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import dev.responsive.kafka.internal.utils.TableName;
import java.util.Collections;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

public class SegmenterTest {

  private static final TableName NAME = new TableName("table");
  private static final String CHANGELOG_TOPIC_NAME = "changelog";

  @Test
  public void shouldReturnCorrectSegmentStartTimestamp() {
    // Given:
    final var retentionPeriod = 10_000;
    final var segmentIntervalMs = 1_000;
    final var segmenter = new Segmenter(retentionPeriod, segmentIntervalMs);

    // Then:
    assertThat(segmenter.segmentStartTimestamp(0), equalTo(0L));
    assertThat(segmenter.segmentStartTimestamp(500), equalTo(0L));
    assertThat(segmenter.segmentStartTimestamp(999), equalTo(0L));
    assertThat(segmenter.segmentStartTimestamp(1_001), equalTo(1_000L));
    assertThat(segmenter.segmentStartTimestamp(2_000), equalTo(2_000L));
  }

  @Test
  public void shouldReturnCorrectActiveSegments() {
    // Given:
    final var retentionPeriod = 10_000;
    final var segmentIntervalMs = 1_000;
    final var segmenter = new Segmenter(retentionPeriod, segmentIntervalMs);

    // When:
    final var activeSegments1 = segmenter.activeSegments(1, 1_999);
    final var activeSegments2 = segmenter.activeSegments(1, 3_000);

    // Then:
    assertThat(activeSegments1, hasSize(2));
    assertThat(activeSegments1, containsInRelativeOrder(
        new Segmenter.SegmentPartition(1, 0),
        new Segmenter.SegmentPartition(1, 1_000)
    ));

    assertThat(activeSegments2, hasSize(4));
    assertThat(activeSegments2, containsInRelativeOrder(
        new Segmenter.SegmentPartition(1, 0),
        new Segmenter.SegmentPartition(1, 1_000),
        new Segmenter.SegmentPartition(1, 2_000),
        new Segmenter.SegmentPartition(1, 3_000)
    ));
  }

  @Test
  public void shouldReturnCorrectRolledSegments() {
    // Given:
    final var retentionPeriod = 10_000;
    final var segmentIntervalMs = 1_000;
    final var segmenter = new Segmenter(retentionPeriod, segmentIntervalMs);
    final var tableName = "test1";
    final var kafkaPartition = 1;

    // When:
    final var rolledSegments1 = segmenter.rolledSegments(tableName, kafkaPartition,
        0, 1_000
    );
    final var rolledSegments2 = segmenter.rolledSegments(tableName, kafkaPartition,
        0, 2_000
    );
    final var rolledSegments3 = segmenter.rolledSegments(tableName, kafkaPartition,
        2_000, 4_000
    );

    // Then:
    assertThat(rolledSegments1, equalTo(new Segmenter.SegmentRoll(
        LongStream.empty(), LongStream.of(1_000)
    )));
    assertThat(rolledSegments2, equalTo(new Segmenter.SegmentRoll(
        LongStream.empty(), LongStream.of(1_000, 2_000)
    )));
    assertThat(rolledSegments3, equalTo(new Segmenter.SegmentRoll(
        LongStream.of(), LongStream.of(3_000, 4_000)
    )));
  }

  @Test
  public void shouldReturnCorrectRange() {
    // Given:
    final var retentionPeriod = 10_000;
    final var segmentIntervalMs = 1_000;
    final var segmenter = new Segmenter(retentionPeriod, segmentIntervalMs);
    final var kafkaPartition = 1;

    // When:
    final var segments1 = segmenter.range(kafkaPartition, 0, 1_000);
    final var segments2 = segmenter.range(kafkaPartition, 0, 3_000);
    final var segments3 = segmenter.range(kafkaPartition, 1_000, 4_000);
    final var reverseSegments1 = segmenter.reverseRange(kafkaPartition, 0, 1_000);
    final var reverseSegments2 = segmenter.reverseRange(kafkaPartition, 0, 3_000);
    final var reverseSegments3 = segmenter.reverseRange(kafkaPartition, 1_000, 4_000);

    // Then:
    assertThat(segments1, hasSize(2));
    assertThat(segments1, containsInRelativeOrder(
        new Segmenter.SegmentPartition(1, 0),
        new Segmenter.SegmentPartition(1, 1_000)
    ));

    assertThat(segments2, hasSize(4));
    assertThat(segments2, containsInRelativeOrder(
        new Segmenter.SegmentPartition(1, 0),
        new Segmenter.SegmentPartition(1, 1_000),
        new Segmenter.SegmentPartition(1, 2_000),
        new Segmenter.SegmentPartition(1, 3_000)
    ));

    assertThat(segments3, hasSize(4));
    assertThat(segments3, containsInRelativeOrder(
        new Segmenter.SegmentPartition(1, 1_000),
        new Segmenter.SegmentPartition(1, 2_000),
        new Segmenter.SegmentPartition(1, 3_000),
        new Segmenter.SegmentPartition(1, 4_000)
    ));

    // *** REVERSE SEGMENTS ***
    Collections.reverse(segments1);
    Collections.reverse(segments2);
    Collections.reverse(segments3);
    assertThat(reverseSegments1, equalTo(segments1));
    assertThat(reverseSegments2, equalTo(segments2));
    assertThat(reverseSegments3, equalTo(segments3));
  }

}
