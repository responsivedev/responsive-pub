/*
 *
 *  * Copyright 2023 Responsive Computing, Inc.
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

package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.internal.utils.SessionKey;
import java.io.Closeable;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;

public interface SessionOperations extends Closeable, RecordBatchingStateRestoreCallback {

  long initialStreamTime();

  void put(final SessionKey key, final byte[] value);

  void delete(final SessionKey key);

  byte[] fetch(final SessionKey key);

  @Override
  void close();
}
