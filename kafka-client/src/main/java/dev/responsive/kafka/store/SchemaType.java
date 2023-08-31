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

package dev.responsive.kafka.store;

public enum SchemaType {

  /**
   * A general purpose key value store that supports fencing
   * zombie writers during a split-brain outage.
   */
  KEY_VALUE,

  /**
   * A fact table schema expects all keys to be recorded with
   * only single "fact" values. Examples of such tables are
   * time-series data and duplicate detection.
   */
  FACT,

  /**
   * A general purpose window store that supports fencing
   * zombie writers during a split-brain outage. Follows
   * regular update/overwrite semantics with exactly one
   * value per window per key.
   */
  WINDOW,

  /**
   * A window-like store that holds individual events corresponding
   * to a specific timestamp, rather than multi-event per-window
   * partial aggregates like a typical window store. Allows duplicate
   * events for a given key and timestamp rather than enforcing
   * update/overwrite semantics.
   * Mainly used for stream-stream joins in the DSL.
   */
  STREAM
}
