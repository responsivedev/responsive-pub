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

  private static final Pattern THREAD_NAME_CONSUMER_REGEX = Pattern.compile("(.*)-consumer$");

  private static final Pattern STREAM_THREAD_REGEX = Pattern.compile(".*-(StreamThread-\\d+)");
  private static final Pattern GLOBAL_THREAD_REGEX = Pattern.compile(".*-(GlobalStreamThread+)");

  /**
   * @param clientId the consumer client id
   * @return the full thread name of the StreamThread that owns this consumer,
   *         of the form [processId]-StreamThread-n
   */
  public static String extractThreadNameFromConsumerClientId(final String clientId) {
    final var threadNameMatcher = THREAD_NAME_CONSUMER_REGEX.matcher(clientId);
    if (!threadNameMatcher.find()) {
      throw new IllegalArgumentException("Attempted to extract threadName from consumer clientId "
                                             + "but no matches were found in " + clientId);
    }
    return threadNameMatcher.group(1);

  }

  /**
   * Compute/extract the full thread id suffix of this stream thread or global thread.
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

}
