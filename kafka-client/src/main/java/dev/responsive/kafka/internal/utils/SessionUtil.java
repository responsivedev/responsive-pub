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

package dev.responsive.kafka.internal.utils;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_THROTTLER_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.RETRY_POLICY_CLASS;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.internal.core.session.throttling.ConcurrencyLimitingRequestThrottler;
import dev.responsive.kafka.internal.db.ResponsiveRetryPolicy;
import java.net.InetSocketAddress;
import javax.annotation.Nullable;

/**
 * This is a utility to help creating a session to connect to the remote
 * Responsive store.
 */
public final class SessionUtil {

  private SessionUtil() {
  }

  public static CqlSession connect(
      final InetSocketAddress address,
      final String datacenter,
      final String keyspace,
      @Nullable final String clientId,
      @Nullable final String clientSecret,
      final int maxConcurrentRequests
  ) {
    final CqlSessionBuilder sessionBuilder = CqlSession.builder()
        .addContactPoint(address)
        .withLocalDatacenter(datacenter);

    if (clientId != null && clientSecret != null) {
      sessionBuilder.withAuthCredentials(clientId, clientSecret);
    } else if (clientId == null ^ clientSecret == null) {
      throw new IllegalArgumentException("Must specify both or neither clientId and clientSecret.");
    }

    return sessionBuilder
        .withConfigLoader(DriverConfigLoader
            .programmaticBuilder()
            .withLong(REQUEST_TIMEOUT, 5000)
            .withClass(RETRY_POLICY_CLASS, ResponsiveRetryPolicy.class)
            .withClass(REQUEST_THROTTLER_CLASS, ConcurrencyLimitingRequestThrottler.class)
            // we just set this to MAX_VALUE as it will be implicitly limited by the
            // number of stream threads * the max number of records being flushed -
            // we do not want to throw an exception if the queue is full as this will
            // cause a rebalance
            .withInt(REQUEST_THROTTLER_MAX_QUEUE_SIZE, Integer.MAX_VALUE)
            .withInt(REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS, maxConcurrentRequests)
            .build())
        .withKeyspace(keyspace)
        .build();
  }

}