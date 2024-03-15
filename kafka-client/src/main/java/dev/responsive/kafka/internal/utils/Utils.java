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

import java.util.regex.Pattern;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Utils {

  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  private static final Pattern STREAM_THREAD_REGEX = Pattern.compile(".*-(StreamThread-\\d+)");
  private static final Pattern GLOBAL_THREAD_REGEX = Pattern.compile(".*-(GlobalStreamThread+)");

  private static final Pattern STREAM_THREAD_INDEX_REGEX = Pattern.compile(".*-StreamThread-(\\d+)");

  /**
   * Compute/extract the full thread id suffix of this stream thread or global thread.
   * If you just want the numerical index of a StreamThread and not the preceding
   * "StreamThread-", use {@link #extractStreamThreadIndex}
   *
   * @return the entire thread suffix, eg "StreamThread-1" or "GlobalStreamThread"
   */
  public static String extractThreadId(final String threadName) {
    final var streamThreadMatcher = STREAM_THREAD_REGEX.matcher(threadName);
    if (streamThreadMatcher.find()) {
      return streamThreadMatcher.group(1);
    }

    final var globalThreadMatcher = GLOBAL_THREAD_REGEX.matcher(threadName);
    if (globalThreadMatcher.find()) {
      return globalThreadMatcher.group(1);
    }

    LOG.warn("Unable to parse the stream thread id, falling back to thread name {}", threadName);
    return threadName;
  }

  /**
   * Extract the stream thread index, ie the set of digits at the very end of a
   * stream thread's name.
   * If you instead want the full thread id suffix, ie including the "StreamThread-"
   * part of the name, see {@link #extractThreadId}.
   *
   * @return the thread index, eg "1" from a thread named "processId-StreamThread-1"
   */
  public static String extractStreamThreadIndex(final String streamThreadName) {
    final var streamThreadMatcher = STREAM_THREAD_INDEX_REGEX.matcher(streamThreadName);
    if (streamThreadMatcher.find()) {
      return streamThreadMatcher.group(1);
    } else {
      LOG.error("Unable to parse thread name and extract index from {}", streamThreadName);
      throw new IllegalStateException(
          "Failed to extract index from stream thread " + streamThreadName
      );
    }
  }

  /**
   * Generates a consistent hashCode for the given {@link ProcessorRecordContext} by
   * This workaround is required due to the actual ProcessorRecordContext class throwing
   * an exception in its #hashCode implementation. This override is intended to discourage the use
   * of this class in hash-based collections, which in turn is because one of its fields,
   * {@link Headers}, is mutable and therefore the hashCode would be susceptible to
   * outside modification.
   * <p>
   * If the headers are guaranteed to be safe-guarded and made effectively immutable,
   * then they are safe to include in the hashCode. If no protections around the headers
   * exist, they should be left out of the hashCode computation. Use the {@code includeHeaders}
   * parameter to hash the headers or exclude them from the result.
   */
  public static int processorRecordContextHashCode(
      final ProcessorRecordContext recordContext,
      final boolean includeHeaders
  ) {
    int result = (int) (recordContext.timestamp() ^ (recordContext.timestamp() >>> 32));
    result = 31 * result + (int) (recordContext.offset() ^ (recordContext.offset() >>> 32));
    result = 31 * result + (recordContext.topic() != null ? recordContext.topic().hashCode() : 0);
    result = 31 * result + recordContext.partition();

    if (includeHeaders) {
      result = 31 * result + recordContext.headers().hashCode();
    }

    return result;
  }

}
