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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.internals.Murmur3;

public class Murmur3Hasher implements Hasher {

  // ensure that the default sub-partitioning hasher is unlikely
  // to be the same as the hasher that was used for the original
  // partition scheme - if the hashers are the same, and the
  // number of sub partitions is equal to the number of original
  // partitions, all keys would be mapped to the same sub-partition
  // and all other sub-partitions would be empty
  static final int SALT = 31;

  @Override
  public Integer apply(final Bytes bytes) {
    return SALT * Murmur3.hash32(bytes.get());
  }
}
