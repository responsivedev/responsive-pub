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

package dev.responsive.db;

import dev.responsive.kafka.store.RemoteWriteResult;
import java.util.concurrent.CompletionStage;

public interface RemoteWriter<K> {

  void insert(final K key, final byte[] value, long timestamp);

  void delete(final K key);

  CompletionStage<RemoteWriteResult> flush();

  RemoteWriteResult setOffset(final long offset);

  int partition();
}
