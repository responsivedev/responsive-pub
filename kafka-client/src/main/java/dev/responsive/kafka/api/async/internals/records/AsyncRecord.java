/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.responsive.kafka.api.async.internals.records;

/**
 * A generic async record and the metadata needed for async processing semantics
 * such as per-key offset ordering.
 * Examples include the actual input records to be processed ({@link ProcessableRecord}),
 * the output records to be forwarded ({@link ForwardableRecord}), and the records that
 * are inserted into a state store ({@link WriteableRecord})
 * <p>
 * Note: the topic/offset for this record correspond to the "input record" that
 * triggered this record, which may be the same record (eg in the case of a
 * {@link ProcessableRecord}) or it may be a different record altogether (eg for a
 * {@link ForwardableRecord} or {@link WriteableRecord})
 */
public interface AsyncRecord<K, V> {

  K key();

  V value();

  /**
   * @return the upstream topic where the corresponding input record came from
   */
  String topic();

  /**
   * @return the offset of the corresponding input record from the upstream topic
   */
  long offset();

}
