/*
 *
 *  * Copyright 2024 Responsive Computing, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package dev.responsive.kafka.internal.db;

import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.stores.RemoteWriteResult;
import java.util.Collection;

public interface FlushManager<K, P> {

  String tableName();

  TablePartitioner<K, P> partitioner();

  RemoteWriter<K, P> createWriter(final P tablePartition);

  void writeAdded(final K key);

  RemoteWriteResult<P> preFlush();

  RemoteWriteResult<P> postFlush(final long consumedOffset);

  default String failedFlushMsg(final long batchOffset) {
    // TODO: remove default and implement!
    return "failed on flushing offset " + batchOffset;
  }

  String logPrefix();

}
