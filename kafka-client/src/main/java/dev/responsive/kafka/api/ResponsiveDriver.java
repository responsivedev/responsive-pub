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

package dev.responsive.kafka.api;

import static dev.responsive.kafka.config.ResponsiveDriverConfig.CLIENT_ID_CONFIG;
import static dev.responsive.kafka.config.ResponsiveDriverConfig.CLIENT_SECRET_CONFIG;
import static dev.responsive.kafka.config.ResponsiveDriverConfig.STORAGE_DATACENTER_CONFIG;
import static dev.responsive.kafka.config.ResponsiveDriverConfig.STORAGE_HOSTNAME_CONFIG;
import static dev.responsive.kafka.config.ResponsiveDriverConfig.STORAGE_PORT_CONFIG;
import static dev.responsive.kafka.config.ResponsiveDriverConfig.TENANT_ID_CONFIG;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import dev.responsive.db.CassandraClient;
import dev.responsive.kafka.config.ResponsiveDriverConfig;
import dev.responsive.utils.SessionUtil;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.streams.StreamsConfig;


/**
 * The {@code ResponsiveDriver} should be instantiated once per JVM
 * and maintains a session and connection to the remote storage server.
 */
public class ResponsiveDriver implements Closeable {

  private final CqlSession session;

  /**
   * @param props the Responsive properties for this session
   * @return a new {@link ResponsiveDriver} and opens connections to remote Responsive servers
   */
  public static ResponsiveDriver connect(final Map<String, Object> props) {
    final Properties properties = new Properties(props.size());
    properties.putAll(props);
    return connect(properties);
  }

  /**
   * @param props the Responsive properties for this session
   * @return a new {@link ResponsiveDriver} and opens connections to remote Responsive servers
   */
  public static ResponsiveDriver connect(final Properties props) {
    final ResponsiveDriverConfig configs = new ResponsiveDriverConfig(props);

    final InetSocketAddress address = InetSocketAddress.createUnresolved(
        configs.getString(STORAGE_HOSTNAME_CONFIG),
        configs.getInt(STORAGE_PORT_CONFIG)
    );

    final String datacenter = configs.getString(STORAGE_DATACENTER_CONFIG);
    final String clientId = configs.getString(CLIENT_ID_CONFIG);
    final Password clientSecret = configs.getPassword(CLIENT_SECRET_CONFIG);
    final String tenant = configs.getString(TENANT_ID_CONFIG);

    return new ResponsiveDriver(
        SessionUtil.connect(
            address,
            datacenter,
            tenant,
            clientId,
            clientSecret == null ? null : clientSecret.value())
    );
  }

  /**
   * @param streamsConfigs the configs for this KafkaStreams application.
   *        Must include all required {@link StreamsConfig StreamsConfigs}
   *
   * @return the driver for a new Streams application. See {@link StreamsApplicationDriver} javadocs
   */
  public StreamsApplicationDriver newApplication(final Map<String, Object> streamsConfigs) {
    return new ResponsiveApplicationDriver(new CassandraClient(session), streamsConfigs);
  }

  @VisibleForTesting
  public ResponsiveDriver(final CqlSession session) {
    this.session = session;
  }

  @Override
  public void close() throws IOException {
    session.close();
  }
}