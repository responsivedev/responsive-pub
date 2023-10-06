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

package dev.responsive.kafka.internal.db;

import dev.responsive.kafka.internal.db.spec.CassandraTableSpec;

/**
 * All possible operations on {@link org.apache.kafka.streams.state.KeyValueStore}
 * and {@link org.apache.kafka.streams.state.WindowStore}, used for checking whether
 * a particular {@link CassandraTableSpec} supports certain operations.
 */
public enum TableOperations {
  PUT,
  PUT_IF_ABSENT,
  PUT_ALL,
  DELETE,

  GET,
  RANGE,
  ALL,

  FETCH,
  FETCH_TIME_RANGE,
  FETCH_KEY_RANGE,
  FETCH_ALL,
  BACK_FETCH,
  BACK_FETCH_TIME_RANGE,
  BACK_FETCH_KEY_RANGE,
  BACK_FETCH_ALL,
}
