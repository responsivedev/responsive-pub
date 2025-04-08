/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.config;

import static dev.responsive.kafka.api.config.ResponsiveConfig.NUM_STANDBYS_OVERRIDE;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.api.stores.ResponsiveDslStoreSuppliers;
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

  public static void validateStreamsConfig(final Map<?, ?> props) {
    final StreamsConfig streamsConfig = streamsConfig(props);

    verifyNoStandbys(streamsConfig);
  }

  /**
   * Validate the config for apps in "compatibility mode" ie without Responsive storage backends
   */
  public static void validateNoStorageStreamsConfig(final Map<?, ?> props) {
    final StreamsConfig streamsConfig = streamsConfig(props);

    verifyNoResponsiveDslStoreSuppliers(streamsConfig);
  }

  private static void verifyNoResponsiveDslStoreSuppliers(final StreamsConfig config)
      throws ConfigException {
    final Class<?> dslStoreSuppliers =
        config.getClass(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG);
    if (dslStoreSuppliers.getName().equals(ResponsiveDslStoreSuppliers.class.getName())) {
      final String errorMsg = String.format(
          "Invalid Streams configuration value for '%s': got %s, "
              + "incompatible with setting '%s' to %s",
          StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG,
          dslStoreSuppliers,
          ResponsiveConfig.STORAGE_BACKEND_TYPE_CONFIG,
          StorageBackend.NONE
      );
      LOG.error(errorMsg);
      throw new ConfigException(errorMsg);
    }
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

  private ResponsiveStreamsConfig(final Map<?, ?> props, final boolean logConfigs) {
    super(props, logConfigs);
  }
}
