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

package dev.responsive.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import dev.responsive.db.ResponsiveRetryPolicy;
import java.net.InetSocketAddress;
import javax.annotation.Nullable;

/** This is a utility to help creating a session to connect to the remote Responsive store. */
public final class SessionUtil {

  private SessionUtil() {}

  public static CqlSession connect(
      final InetSocketAddress address,
      final String datacenter,
      final String keyspace,
      @Nullable final String clientId,
      @Nullable final String clientSecret) {
    final CqlSessionBuilder sessionBuilder =
        CqlSession.builder().addContactPoint(address).withLocalDatacenter(datacenter);

    if (clientId != null && clientSecret != null) {
      sessionBuilder.withAuthCredentials(clientId, clientSecret);
    } else if (clientId == null ^ clientSecret == null) {
      throw new IllegalArgumentException("Must specify both or neither clientId and clientSecret.");
    }

    return sessionBuilder
        .withConfigLoader(
            DriverConfigLoader.programmaticBuilder()
                .withLong(DefaultDriverOption.REQUEST_TIMEOUT, 5000)
                .withClass(DefaultDriverOption.RETRY_POLICY_CLASS, ResponsiveRetryPolicy.class)
                .build())
        .withKeyspace(keyspace)
        .build();
  }
}
