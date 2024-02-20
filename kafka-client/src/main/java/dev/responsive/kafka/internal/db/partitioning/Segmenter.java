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

import static java.util.Collections.emptyList;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class for segment-based partitioners where kafka partitions are mapped to a subset
 * of remote partitions divided up into "segments" that correspond to a range of timestamps.
 * By deleting a table partition we can "drop" a full segment at a time, which can be done if
 * (and only if) the stream-time advances such that the highest timestamp in the range is now
 * outside the retention period. This is intended for segmented WindowStores, but could be
 * extended in the future to other timeseries and/or ttl types of state stores.
 * <p>
 * Each segment covers a specific time range in milliseconds. If t_n is the start time of the
 * nth segment, then that segment covers all timestamps from t_s - t_s + segmentInterval.
 * For each segment id there are N physical table partitions, where N is the number of kafka
 * partitions. Each physical state store instance should only be interacting with a single kafka
 * partition, but will generally have multiple active segments for each kafka partition.
 * <p>
 * For a given kafka partition, the total number of segments/table partitions is determined by
 * the store-level config for num_segments, which controls the width of each segment according
 * to the relationship num_segments * segmentInterval = retentionPeriod.
 * Note: the actual number of physical segments at any given time is likely to be num_segments + 1,
 * for two reasons. First, if the configured numSegments does not evenly divide the retentionPeriod,
 * an additional partial segment will always be required to cover any remainder of the full
 * retention period left. Second, at any given point in time it's likely that the time range
 * covered by the oldest segment is partially expired, while the upper end of the newest segment
 * may not yet be active/filled.
 * <p>
 * When stream-time for a given kafka partition advances, a tombstone is sent to delete the entire
 * oldest physical segment (or segments, if it has advanced by more than the segmentInterval).
 * At the same time, a new physical segment (or segments) will be created and the corresponding
 * remote partition initialized.
 * <p>
 * Let's look at an example case, at a particular moment in time, with the following configuration:
 * N = 4
 * retentionPeriod = 100ms
 * numSegments = 3
 * segmentInterval = 33
 * <p>
 * kafkaPartition | stream-time | minValidTs |    active segment time bounds   |  segmentId ||
 *           0    |     16      |      0     | 0-32                            | 0          ||
 *           1    |     88      |      0     | 0-32, 33-65, 66-98              | 0, 1, 2    ||
 *           2    |     101     |      2     | 0-32, 33-65, 66-98, 99-131      | 0, 1, 2, 3 ||
 *           3    |     169     |      70    | 66-98, 99-131, 132-164, 165-197 | 1, 2, 3, 4 ||
 * <p>
 * NOTE: because we are already dividing up the partition space into segments, we don't further
 * split things into true sub-partitions based on key. Each kafka partition still maps to multiple
 * table partitions which should help with the parallelism, but unlike the subpartitioner, the
 * segment-partitioning scheme is not static and temporal rather than key-based. If data is skewed
 * in time, this might result in uneven partitions and a need to further subdivide the partition
 * space.
 * <p>
 * For the time being, we simply recommend that users configure the number of segments
 * similarly to how they would configure the number of sub-partitions for a key-value store.
 */
public class Segmenter {

  private static final Logger LOG = LoggerFactory.getLogger(Segmenter.class);

  public static final long UNINITIALIZED_STREAM_TIME = -1L;

  private final long retentionPeriodMs;
  private final long segmentIntervalMs;

  public static class SegmentPartition {
    public final int tablePartition;
    public final long segmentId;

    public SegmentPartition(final int tablePartition, final long segmentId) {
      this.tablePartition = tablePartition;
      this.segmentId = segmentId;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final SegmentPartition that = (SegmentPartition) o;

      if (tablePartition != that.tablePartition) {
        return false;
      }
      return segmentId == that.segmentId;
    }

    @Override
    public int hashCode() {
      int result = tablePartition;
      result = 31 * result + (int) (segmentId ^ (segmentId >>> 32));
      return result;
    }

    @Override
    public String toString() {
      return "SegmentPartition{"
          + "tablePartition=" + tablePartition
          + ", segmentId=" + segmentId
          + '}';
    }
  }

  public Segmenter(
      final long retentionPeriodMs,
      final long segmentIntervalMs
  ) {
    this.retentionPeriodMs = retentionPeriodMs;
    this.segmentIntervalMs = segmentIntervalMs;
    if (retentionPeriodMs <= 0L || segmentIntervalMs <= 0L) {
      LOG.error("Segment values should all be positive, got retentionPeriod={}ms, "
          + "segmentInterval={}ms", retentionPeriodMs, segmentIntervalMs
      );
      throw new IllegalStateException("Segment partitioner received a negative or zero value");
    }

    LOG.info(
        "Created segment partitioner with retentionPeriod={}ms, segmentInterval={}ms",
        retentionPeriodMs,
        segmentIntervalMs
    );
  }

  public long segmentIntervalMs() {
    return segmentIntervalMs;
  }

  /**
   * Return all active segments for the given stream-time and retention period
   *
   * @param kafkaPartition the original partition in kafka
   * @param streamTime     the lowest timestamp in the fetched range
   * @return               all remote partitions for active segments of this kafka partition
   */
  public List<SegmentPartition> activeSegments(
      final int kafkaPartition,
      final long streamTime
  ) {
    if (streamTime == UNINITIALIZED_STREAM_TIME) {
      return emptyList();
    } else {
      return range(kafkaPartition, minValidTs(streamTime), streamTime);
    }
  }

  /**
   * Return all active segments that could contain data with a timestamp in the specified range
   * The {@code timeFrom} parameter should already account for the retention
   *
   * @param kafkaPartition the original partition in kafka
   * @param timeFrom       the lowest timestamp in the fetched range (inclusive)
   * @param timeTo         the highest timestamp in the fetched range (inclusive)
   * @return               all remote partitions for segments in this range for this kafka partition
   */
  public List<SegmentPartition> range(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    return LongStream.range(segmentId(timeFrom), segmentId(timeTo) + 1)
        .mapToObj(segmentId -> new SegmentPartition(kafkaPartition, segmentId))
        .collect(Collectors.toList());
  }

  /**
   * Return all active segments that could contain data with a timestamp in the specified range,
   * in reverse order
   * The{@code timeFrom} parameter should already account for the retention
   *
   * @param kafkaPartition the original partition in kafka
   * @param timeFrom       the lowest timestamp in the fetched range
   * @param timeTo         the highest timestamp in the fetched range
   * @return               all remote partitions for segments in this range for this kafka partition
   */
  public List<SegmentPartition> reverseRange(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    return LongStream.range(segmentId(timeFrom), segmentId(timeTo) + 1)
        .boxed()
        .sorted(Collections.reverseOrder())
        .map(segmentId -> new SegmentPartition(kafkaPartition, segmentId))
        .collect(Collectors.toList());
  }

  public SegmentRoll rolledSegments(
      final String tableName,
      final int kafkaPartition,
      final long oldStreamTime,
      final long newStreamTime
  ) {
    final long oldMaxActiveSegment = segmentId(oldStreamTime);
    final long newMaxActiveSegment = segmentId(newStreamTime);

    final long oldMinActiveSegment = segmentId(minValidTs(oldStreamTime));
    final long newMinActiveSegment = segmentId(minValidTs(newStreamTime));

    // Special case where this is the first record we've received
    if (oldStreamTime == UNINITIALIZED_STREAM_TIME) {
      final LongStream segmentsToExpire = LongStream.empty();

      final LongStream segmentsToCreate = LongStream.range(
          newMinActiveSegment,
          newMaxActiveSegment + 1 // add 1 since the upper bound is exclusive
      );

      LOG.info("Initializing stream-time for table {} to {}ms and creating segments: [{}-{}]",
          tableName, newStreamTime, newMinActiveSegment, newMaxActiveSegment
      );

      return new SegmentRoll(
          segmentsToExpire,
          segmentsToCreate
      );
    } else {
      final LongStream segmentsToExpire = LongStream.range(
          oldMinActiveSegment,
          newMinActiveSegment
      );

      final LongStream segmentsToCreate = LongStream.range(
          oldMaxActiveSegment + 1, // inclusive: add 1 b/c the old max segment should already exist
          newMaxActiveSegment + 1  // exclusive: add 1 to create segment for highest valid timestamp
      );

      if (newMinActiveSegment > oldMinActiveSegment) {
        LOG.info("{}[{}] Advancing stream-time from {}ms to {}ms and rolling segments with "
                + "expiredSegments: [{}-{}] and newSegments: [{}-{}]",
            tableName, kafkaPartition, oldStreamTime, newStreamTime, oldMinActiveSegment,
            newMinActiveSegment, oldMaxActiveSegment + 1, newMaxActiveSegment
        );
      }
      return new SegmentRoll(
          segmentsToExpire,
          segmentsToCreate
      );
    }
  }

  public long segmentId(final long windowTimestamp) {
    return Long.max(0, windowTimestamp / segmentIntervalMs);
  }

  private long minValidTs(final long streamTime) {
    return streamTime - retentionPeriodMs + 1;
  }

  public static class SegmentRoll {
    private final List<Long> segmentsToExpire;
    private final List<Long> segmentsToCreate;

    public SegmentRoll(final LongStream segmentsToExpire, final LongStream segmentsToCreate) {
      this.segmentsToExpire = segmentsToExpire.boxed().collect(Collectors.toUnmodifiableList());
      this.segmentsToCreate = segmentsToCreate.boxed().collect(Collectors.toUnmodifiableList());
    }

    public List<Long> segmentsToExpire() {
      return segmentsToExpire;
    }

    public List<Long> segmentsToCreate() {
      return segmentsToCreate;
    }

    @Override
    public String toString() {
      final int numExpired = segmentsToExpire.size();
      final String expired = numExpired == 0
          ? "[]"
          : String.format("[%d-%d]", segmentsToExpire.get(0), segmentsToExpire.get(numExpired - 1));

      final int numCreated = segmentsToCreate.size();
      final String created = numCreated == 0
          ? "[]"
          : String.format("[%d-%d]", segmentsToCreate.get(0), segmentsToCreate.get(numCreated - 1));

      return String.format("SegmentRoll: expired segment(s)=%s, new segments(s)=%s",
          expired, created
      );
    }
  }
}
