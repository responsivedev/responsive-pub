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

import static dev.responsive.kafka.internal.db.ColumnName.METADATA_SEGMENT_ID;

import dev.responsive.kafka.api.stores.ResponsiveWindowParams;
import dev.responsive.kafka.internal.db.partitioning.SegmentPartitioner.SegmentPartition;
import dev.responsive.kafka.internal.utils.Stamped;
import dev.responsive.kafka.internal.utils.StoreUtil;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.kafka.common.utils.Bytes;

/**
 * A segment-based partitioner where kafka partitions are mapped to a subset of remote partitions
 * divided up into "segments" that correspond to a range of timestamps. By deleting a table
 * partition we can "drop" a full segment at a time, which can be done if (and only if) the
 * stream-time advances such that the highest timestamp in the range is now outside the retention
 * period. This is intended for segmented WindowStores, but could be extended in the future to
 * other timeseries and/or ttl types of state stores.
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
public class SegmentPartitioner implements ResponsivePartitioner<Stamped<Bytes>, SegmentPartition> {

  private final long retentionPeriodMs;
  private final long segmentIntervalMs;

  public static class SegmentPartition {
    public final int partitionKey;
    public final long segmentId;

    public SegmentPartition(final int partitionKey, final long segmentId) {
      this.partitionKey = partitionKey;
      this.segmentId = segmentId;
    }
  }

  public static SegmentPartitioner create(
      final ResponsiveWindowParams params
  ) {
    final long segmentInterval =
        StoreUtil.computeSegmentInterval(params.retentionPeriod(), params.numSegments());
    return new SegmentPartitioner(params.retentionPeriod(), segmentInterval);
  }

  public SegmentPartitioner(final long retentionPeriodMs, final long segmentIntervalMs) {
    this.retentionPeriodMs = retentionPeriodMs;
    this.segmentIntervalMs = segmentIntervalMs;
  }

  @Override
  public SegmentPartition tablePartition(final int kafkaPartition, final Stamped<Bytes> key) {
    // We don't subdivide the kafka partition any further since it's assumed there are
    // sufficient segments to get good parallelism, but we could make both configurable
    return new SegmentPartition(kafkaPartition, segmentId(key.stamp));
  }

  @Override
  public SegmentPartition metadataTablePartition(final int kafkaPartition) {
    return new SegmentPartition(kafkaPartition, METADATA_SEGMENT_ID);
  }

  /**
   * Return all possible table partitions that could hold data.
   * Note, the {@code timeFrom} parameter should already account for the retention
   *
   * @param kafkaPartition the original partition in kafka
   * @param timeFrom       the lowest timestamp in the fetched range
   * @param timeTo         the highest timestamp in the fetched range
   * @return               all remote partitions for segments in this range for this kafka partition
   */
  public Iterable<SegmentPartition> range(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    return LongStream.range(segmentId(timeFrom), segmentId(timeTo))
        .mapToObj(segmentId -> new SegmentPartition(kafkaPartition, segmentId))
        .collect(Collectors.toList());
  }

  /**
   * Return all possible table partitions that could hold data in reverse order.
   * Note,the {@code timeFrom} parameter should already account for the retention
   *
   * @param kafkaPartition the original partition in kafka
   * @param timeFrom       the lowest timestamp in the fetched range
   * @param timeTo         the highest timestamp in the fetched range
   * @return               all remote partitions for segments in this range for this kafka partition
   */
  public Iterable<SegmentPartition> reverseRange(
      final int kafkaPartition,
      final long timeFrom,
      final long timeTo
  ) {
    return LongStream.range(segmentId(timeFrom), segmentId(timeTo))
        .boxed()
        .sorted(Collections.reverseOrder())
        .map(segmentId -> new SegmentPartition(kafkaPartition, segmentId))
        .collect(Collectors.toList());
  }

  public SegmentRoll rolledSegments(final long oldStreamTime, final long newStreamTime) {
    final long oldMinValidTs = oldStreamTime - retentionPeriodMs + 1;
    final long newMinValidTs = newStreamTime - retentionPeriodMs + 1;

    // The lower bound is inclusive and the upper bound exclusive for #range, so we have to
    // add 1 to the segment id for the range of segments to create
    return new SegmentRoll(
        LongStream.range(segmentId(oldMinValidTs), segmentId(newMinValidTs)).toArray(),
        LongStream.range(segmentId(oldStreamTime) + 1, segmentId(newMinValidTs) + 1).toArray()
    );
  }

  private int segmentId(final long windowTimestamp) {
    return (int) (windowTimestamp / segmentIntervalMs);
  }

  public static class SegmentRoll {
    public final long[] segmentsToExpire;
    public final long[] segmentsToCreate;

    public SegmentRoll(final long[] segmentsToExpire, final long[] segmentsToCreate) {
      this.segmentsToExpire = segmentsToExpire;
      this.segmentsToCreate = segmentsToCreate;
    }

    @Override
    public String toString() {
      return String.format("SegmentRoll will remove expired segments: '(%s)'"
                               + " and creating new segments '(%s)'",
                           Arrays.toString(segmentsToExpire), Arrays.toString(segmentsToCreate));
    }
  }
}
