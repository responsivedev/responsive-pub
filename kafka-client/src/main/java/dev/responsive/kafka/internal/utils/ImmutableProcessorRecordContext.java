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

package dev.responsive.kafka.internal.utils;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

/**
 * Essentially acts like an immutable form of the {@link ProcessorRecordContext}
 * that implements #hashCode and can be used in collections. This class simply
 * wraps a single {@link ProcessorRecordContext} instance and delegates all APIs,
 * except for any getters that would return mutable types.
 * Specifically, it saves a copy of the {@link Headers} field and replaces that
 * of the delegate, since the {@link Headers} class is theoretically modifiable.
 * See this discussion for more context:
 * https://github.com/apache/kafka/pull/6602/files#r278810522
 */
public class ImmutableProcessorRecordContext {

  private final ProcessorRecordContext delegate;

  // our copy of the original Headers from the delegate
  // won't be modified when the delegate's headers are, but is still mutable and so
  // must be protected by making a new copy
  private final Header[] headers;

  public ImmutableProcessorRecordContext(
      final ProcessorRecordContext recordContext
  ) {
    this.delegate = recordContext;
    this.headers = recordContext.headers().toArray();
  }



  @Override
  public int hashCode() {
    int result = (int) (timestamp() ^ (timestamp() >>> 32));
    result = 31 * result + (int) (offset() ^ (offset() >>> 32));
    result = 31 * result + (topic() != null ? topic().hashCode() : 0);
    result = 31 * result + partition();
    result = 31 * result + headers().hashCode();
    return result;
  }

  @Override
  public String topic() {
    return delegate.topic();
  }

  @Override
  public int partition() {
    return delegate.partition();
  }

  @Override
  public long offset() {
    return delegate.offset();
  }

  @Override
  public long timestamp() {
    return delegate.timestamp();
  }

  // Since the Headers class is effectively just a wrapper around a basic Header[] array,
  // and does not make a copy of the array in the constructor, we have to make another
  // copy here and protect our stashed copy of the delegate's original Headers by
  // passing in a clone to the Headers constructor, instead of our base copy of the array
  @Override
  public Headers headers() {
    return new RecordHeaders(headers.clone());
  }
}
