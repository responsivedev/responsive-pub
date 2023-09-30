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

package dev.responsive.internal.db.partitioning;

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
