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

import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_APPLICATION_ID_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_ENV_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_ORG_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.AT_LEAST_ONCE;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.ResponsiveMode;
import dev.responsive.kafka.api.config.StorageBackend;
import java.util.Locale;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Internal utility to make it easier to extract some values from {@link ResponsiveConfig}
 * without making those methods part of our public API.
 *
 * @implNote please keep this in the {@code .internal} package
 */
public class ConfigUtils {

  private ConfigUtils() {
    /* Empty constructor for public class */
  }

  public static String cassandraKeyspace(final ResponsiveConfig config) {
    return config.getString(RESPONSIVE_ORG_CONFIG) + "_" + config.getString(RESPONSIVE_ENV_CONFIG);
  }

  public static StorageBackend storageBackend(final ResponsiveConfig config) {
    final var backend = config
        .getString(ResponsiveConfig.STORAGE_BACKEND_TYPE_CONFIG)
        .toUpperCase(Locale.ROOT);

    return StorageBackend.valueOf(backend);
  }

  public static ResponsiveMode responsiveMode(final ResponsiveConfig config) {
    final var mode = config
        .getString(ResponsiveConfig.RESPONSIVE_MODE)
        .toUpperCase(Locale.ROOT);

    return ResponsiveMode.valueOf(mode);
  }

  public static String responsiveAppId(
      final StreamsConfig streamsConfig,
      final ResponsiveConfig responsiveConfig
  ) {
    if (responsiveConfig.originals().containsKey(RESPONSIVE_APPLICATION_ID_CONFIG)) {
      return responsiveConfig.getString(RESPONSIVE_APPLICATION_ID_CONFIG);
    }

    return streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG);
  }

  public static boolean eosEnabled(final StreamsConfig configs) {
    return !AT_LEAST_ONCE.equals(configs.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));
  }

}
