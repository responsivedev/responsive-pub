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

import org.apache.kafka.common.utils.Bytes;

/**
 * Partitioner that is used only for global tables
 */
public class GlobalPartitioner<K> extends DefaultPartitioner<K> {

  public GlobalPartitioner() {
    super(0);
  }

  @Override
  public boolean belongs(final Bytes key, final int kafkaPartition) {
    // never filter out anything with global tables
    return true;
  }

}
