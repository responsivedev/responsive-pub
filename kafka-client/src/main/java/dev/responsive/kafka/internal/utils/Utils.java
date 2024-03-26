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

}
