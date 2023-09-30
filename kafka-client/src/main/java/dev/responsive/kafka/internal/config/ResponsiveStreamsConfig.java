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

package dev.responsive.kafka.internal.config;

import static dev.responsive.kafka.api.config.ResponsiveConfig.NUM_STANDBYS_OVERRIDE;

import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponsiveStreamsConfig extends StreamsConfig {

  private static final Logger LOG = LoggerFactory.getLogger(ResponsiveStreamsConfig.class);

  /**
   * NOTE: always use this to create instances of {@link StreamsConfig} instead of creating
   * them directly, to avoid unnecessarily logging the config over and over. The StreamsConfig
   * will be logged when we invoke the {@link org.apache.kafka.streams.KafkaStreams} constructor,
   * so we should never log it ourselves.
   */
  public static ResponsiveStreamsConfig streamsConfig(final Map<?, ?> props) {
    return new ResponsiveStreamsConfig(props, false);
  }

  public static void validateStreamsConfig(final StreamsConfig streamsConfig) {
    verifyNoStandbys(streamsConfig);
    verifyNotEosV1(streamsConfig);
  }

  static void verifyNoStandbys(final StreamsConfig config) throws ConfigException {
    // In this case the default and our desired value are both 0, so we only need to check for
    // accidental user overrides
    final int numStandbys = config.getInt(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG);
    if (numStandbys != 0) {
      final String errorMsg = String.format(
          "Invalid Streams configuration value for '%s': got %d, expected '%d'",
          StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG,
          numStandbys,
          NUM_STANDBYS_OVERRIDE
      );
      LOG.error(errorMsg);
      throw new ConfigException(errorMsg);
    }
  }

  @SuppressWarnings("deprecation")
  static void verifyNotEosV1(final StreamsConfig config) throws ConfigException {
    if (EXACTLY_ONCE.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG))) {
      throw new ConfigException("Responsive driver can only be used with ALOS/EOS-V2");
    }
  }

  private ResponsiveStreamsConfig(final Map<?, ?> props, final boolean logConfigs) {
    super(props, logConfigs);
  }
}
