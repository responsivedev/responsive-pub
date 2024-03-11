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

package dev.responsive.kafka.api.async.internals;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

/**
 * Essentially a clone of the {@link ProcessorRecordContext} that is guaranteed to be
 * immutable and implements #hashCode to allow for use in collections. Makes a copy of
 * the passed-in {@link ProcessorRecordContext} with the exact same fields.
 * <p>
 * Note: the actual {@link ProcessorRecordContext} class is not in fact mutable, at
 * least not anymore. However, due to what appears to be an out-of-date restriction,
 * the #hashCode method is deprecated and throws an exception. So we have to override
 * it ourselves until we can upgrade to a version with the fix.
 * See https://github.com/apache/kafka/pull/15508
 */
public class AsyncProcessorRecordContext extends ProcessorRecordContext {

  public AsyncProcessorRecordContext(
      final ProcessorRecordContext recordContext
  ) {
    super(recordContext.timestamp(),
          recordContext.offset(),
          recordContext.partition(),
          recordContext.topic(),
          recordContext.headers());
  }

  @SuppressWarnings("deprecation") // see note in class javadocs above
  @Override
  public int hashCode() {
    int result = (int) (super.timestamp() ^ (super.timestamp() >>> 32));
    result = 31 * result + (int) (super.offset() ^ (super.offset() >>> 32));
    result = 31 * result + (super.topic() != null ? super.topic().hashCode() : 0);
    result = 31 * result + super.partition();
    result = 31 * result + (super.headers() != null ? super.headers().hashCode() : 0);
    return result;
  }
}
