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

package dev.responsive.kafka.internal.config;

import static dev.responsive.kafka.api.config.ResponsiveConfig.CONTROLLER_ENDPOINT_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_APPLICATION_ID_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_ENV_CONFIG;
import static dev.responsive.kafka.api.config.ResponsiveConfig.RESPONSIVE_ORG_CONFIG;

import dev.responsive.kafka.api.config.CompatibilityMode;
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

  public static String tenant(final ResponsiveConfig config) {
    return config.getString(RESPONSIVE_ORG_CONFIG) + "-" + config.getString(RESPONSIVE_ENV_CONFIG);
  }

  public static String cassandraKeyspace(final ResponsiveConfig config) {
    return config.getString(RESPONSIVE_ORG_CONFIG) + "_" + config.getString(RESPONSIVE_ENV_CONFIG);
  }

  public static String controllerUri(final ResponsiveConfig config) {
    final var controllerUri = config.getString(CONTROLLER_ENDPOINT_CONFIG);
    return tenant(config) + "." + controllerUri;
  }

  public static StorageBackend storageBackend(final ResponsiveConfig config) {
    final var backend = config
        .getString(ResponsiveConfig.STORAGE_BACKEND_TYPE_CONFIG)
        .toUpperCase(Locale.ROOT);

    return StorageBackend.valueOf(backend);
  }

  public static CompatibilityMode compatibilityMode(final ResponsiveConfig config) {
    final var backend = config
        .getString(ResponsiveConfig.COMPATIBILITY_MODE_CONFIG)
        .toUpperCase(Locale.ROOT);

    return CompatibilityMode.valueOf(backend);
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

}
