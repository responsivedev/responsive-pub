/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.internal.utils;

import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:linelength")
public final class Utils {

  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  private static final Pattern STREAM_THREAD_ID_PRODUCER_REGEX = Pattern.compile(".*-(StreamThread-\\d+)-producer");
  private static final Pattern STREAM_THREAD_ID_CONSUMER_REGEX = Pattern.compile(".*-(StreamThread-\\d+)-consumer");
  private static final Pattern STREAM_THREAD_ID_RESTORE_CONSUMER_REGEX = Pattern.compile(".*-(StreamThread-\\d+)-restore-consumer");

  private static final Pattern STREAM_THREAD_NAME_PRODUCER_REGEX = Pattern.compile("(.*)-producer$");
  private static final Pattern STREAM_THREAD_NAME_CONSUMER_REGEX = Pattern.compile("(.*)-consumer$");

  private static final Pattern STREAM_THREAD_ID_REGEX = Pattern.compile(".*-(StreamThread-\\d+)");
  private static final Pattern GLOBAL_THREAD_ID_REGEX = Pattern.compile(".*-(GlobalStreamThread+)");

  /**
   * @param clientId the producer client id
   * @return the extracted StreamThread id, of the form "StreamThread-n"
   */
  public static String extractThreadIdFromProducerClientId(final String clientId) {
    final var match = STREAM_THREAD_ID_PRODUCER_REGEX.matcher(clientId);
    if (!match.find()) {
      LOG.error("Unable to parse thread id from producer client id = {}", clientId);
      throw new RuntimeException("unexpected client id " + clientId);
    }
    return match.group(1);
  }

  /**
   * @param clientId the consumer client id
   * @return the extracted StreamThread id, of the form "StreamThread-n"
   */
  public static String extractThreadIdFromConsumerClientId(final String clientId) {
    final var match = STREAM_THREAD_ID_CONSUMER_REGEX.matcher(clientId);
    if (!match.find()) {
      LOG.error("Unable to parse thread id from consumer client id = {}", clientId);
      throw new RuntimeException("unexpected client id " + clientId);
    }
    return match.group(1);
  }

  /**
   * @param clientId the restore consumer client id
   * @return the extracted StreamThread id, of the form "StreamThread-n"
   */
  public static String extractThreadIdFromRestoreConsumerClientId(final String clientId) {
    final var match = STREAM_THREAD_ID_RESTORE_CONSUMER_REGEX.matcher(clientId);
    if (!match.find()) {
      LOG.error("Unable to parse thread id from restore consumer client id = {}", clientId);
      throw new RuntimeException("unexpected client id " + clientId);
    }
    return match.group(1);
  }

  /**
   * @param clientId the producer client id
   * @return the full thread name of the StreamThread that owns this producer,
   *         of the form [processId]-StreamThread-n
   */
  public static String extractThreadNameFromProducerClientId(final String clientId) {
    final var threadNameMatcher = STREAM_THREAD_NAME_PRODUCER_REGEX.matcher(clientId);
    if (!threadNameMatcher.find()) {
      throw new IllegalArgumentException("Attempted to extract threadName from producer clientId "
                                             + "but no matches were found in " + clientId);
    }
    return threadNameMatcher.group(1);
  }

  /**
   * @param clientId the consumer client id
   * @return the full thread name of the StreamThread that owns this consumer,
   *         of the form [processId]-StreamThread-n
   */
  public static String extractThreadNameFromConsumerClientId(final String clientId) {
    final var threadNameMatcher = STREAM_THREAD_NAME_CONSUMER_REGEX.matcher(clientId);
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
  public static String extractThreadIdFromThreadName(final String threadName) {
    final var streamThreadMatcher = STREAM_THREAD_ID_REGEX.matcher(threadName);
    if (streamThreadMatcher.find()) {
      return streamThreadMatcher.group(1);
    }

    final var globalThreadMatcher = GLOBAL_THREAD_ID_REGEX.matcher(threadName);
    if (globalThreadMatcher.find()) {
      return globalThreadMatcher.group(1);
    }

    LOG.warn("Unable to parse the stream thread id, falling back to thread name {}", threadName);
    return threadName;
  }
}
