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

package dev.responsive.kafka.internal.db;

import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.api.stores.ResponsiveWindowParams;
import dev.responsive.kafka.internal.db.partitioning.Segmenter.SegmentPartition;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.db.spec.DefaultTableSpec;
import dev.responsive.kafka.internal.db.spec.RemoteTableSpec;
import dev.responsive.kafka.internal.stores.TtlResolver;
import dev.responsive.kafka.internal.utils.WindowedKey;
import java.util.Optional;
import org.apache.kafka.common.utils.Bytes;

/**
 * Translates {@link dev.responsive.kafka.api.stores.ResponsiveKeyValueParams}
 * and {@link dev.responsive.kafka.api.stores.ResponsiveWindowParams} into
 * corresponding {@link RemoteTableSpec} instances.
 *
 * <p>Do not move functionality from this class into the above classes since
 * those are public classes and it's better to keep this functionality
 * internal.</p>
 */
public class RemoteTableSpecFactory {

  public static RemoteTableSpec fromKVParams(
      final ResponsiveKeyValueParams params,
      final TablePartitioner<Bytes, Integer> partitioner,
      final Optional<TtlResolver<?, ?>> ttlResolver
  ) {
    return new DefaultTableSpec(
        params.name().tableName(),
        partitioner,
        ttlResolver
    );
  }

  public static RemoteTableSpec fromWindowParams(
      final ResponsiveWindowParams params,
      final TablePartitioner<WindowedKey, SegmentPartition> partitioner
  ) {
    return new DefaultTableSpec(params.name().tableName(), partitioner, Optional.empty());
  }

}
