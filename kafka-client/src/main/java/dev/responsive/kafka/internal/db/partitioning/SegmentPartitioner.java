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
import java.util.stream.Stream;
import org.apache.kafka.common.utils.Bytes;

/**
 * A segment-based partitioner where kafka partitions are mapped to a subset of remote partitions
 * divided up into "segments" that correspond to a range of timestamps. By deleting subpartitions
 * we can "drop" a full segment at a time, which can be done if (and only if) the stream-time
 * advances such that the highest timestamp in the range is now outside the retention period.
 * This is used by WindowStores.
 * <p>
 * Each logical segment covers a specific time range in milliseconds. If t_n is the start time of
 * the nth segment, then that segment covers all timestamps from t_s - t_s + segmentInterval.
 * For each logical segment there are N physical segments, where N is the number of kafka partitions
 * <p>
 * As such, the overall layout is in repeating sets of N segments and the total number of remote
 * partitions is N * num_segments, where num_segments = retentionPeriod / segmentInterval.
 * Note: the actual number of physical segments/remote partitions is likely to be
 * N * (num_segments + 1), for two reasons. First, if the configured numSegments does not evenly
 * divide the retentionPeriod, an additional partial segment will always be required to cover any
 * remainder of the full retention period left. Second, at any given point in time it's likely that
 * the time range covered by the oldest segment is partially expired and the newest segment is not
 * yet active in full. When stream-time for a given kafka partition advances, a tombstone is sent
 * to delete the entire oldest physical segment. At the same time, a new physical segment will be
 * created and the corresponding remote partition initialized.
 * <p>
 * Note how we specify "physical" segment in the previous statement -- because stream-time advances
 * individually for each kafka partition, the actual physical segments of a given logical segment
 * will be created and dropped out of sync with each other. There is no guarantee that physical
 * segments are aligned or adjacent to other physical segments of the same logical segment. In
 * other words, a given logical segment/timespan may have anywhere between 0 and N physical segments
 * active at any point in time.
 * Within a given kafka partition, however, the active logical segments should always be aligned
 * and adjacent. See the example below for a more concrete analysis
 * <p>
 * Let's look at an example case, at a particular moment in time, with the following configuration:
 * N = 4
 * retentionPeriod = 100ms
 * numSegments = 3
 * segmentInterval = 33
 * <p>
 * Now we define a few terms:
 * k_p == kafka partition
 * minTs == the minimum valid (unexpired) timestamp
 * logical segments == timestamp range of active segments
 * ls_id == logical segment ids
 * ps_id == physical segment ids, equal to the table partitions at a given point in time
 * <p>
 * || k_p | stream-time | minTs |   logical segments              |   ls_id    |  ps_id  ||
 * ||  0  |   16        |  0    | 0-32                            | 0          |  0
 * ||  1  |   88        |  0    | 0-32, 33-65, 66-98              | 0, 1, 2    | 1, 5, 9
 * ||  2  |   101       |  1    | 0-32, 33-65, 66-98, 99-131      | 0, 1, 2, 3 | 2, 6, 10
 * ||  3  |   168       |  68   | 66-98, 99-131, 132-164, 165-197 | 1, 2, 3, 4 |
 * <p>
 * From the example above, it should be clear that the table partition for a given windowed key
 * can be found by first determining the logical segment range that the timestamp lies in, and
 * converting this to a logical segment id by dividing the timestamp by the segment interval.
 * To get the physical segment id, and thus table partition, you must then multiply the logical
 * segment id by the number of total kafka partitions and then add the specific kafka partition
 * number to get the final table partition at a given time, for a given partition
 * <p>
 * NOTE: because we are already dividing up the partition space into segments, we don't further
 * split things into true sub-partitions based on key. Each kafka partition still maps to multiple
 * table partitions which should help with the parallelism, but unlike the subpartitioner, the
 * segment-partitioning scheme is not static and temporal rather than key-based. If data is skewed
 * in time, this might result in uneven partitions and a need to further subdivide the partition
 * space.
 * <p>
 * For the time being, however, we simply recommend that users configure the number of segments
 * similarly to how they would configure the number of sub-partitions for a key-value store.
 */
public class SegmentPartitioner implements ResponsivePartitioner<Stamped<Bytes>, SegmentPartition> {

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
    return new SegmentPartitioner(segmentInterval);
  }

  public SegmentPartitioner(final long segmentIntervalMs) {
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

  private int segmentId(final long windowTimestamp) {
    return (int) (windowTimestamp / segmentIntervalMs);
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
    // TODO
    throw new UnsupportedOperationException("TODO");
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
    // TODO
    throw new UnsupportedOperationException("TODO");
  }

}
