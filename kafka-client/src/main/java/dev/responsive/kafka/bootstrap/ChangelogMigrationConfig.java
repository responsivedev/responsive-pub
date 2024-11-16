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

package dev.responsive.kafka.bootstrap;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class ChangelogMigrationConfig extends AbstractConfig {

  // ----------------- general configurations ---------------------------

  public static final String CHANGELOG_TOPIC_CONFIG = "responsive.migration.changelog.topic";
  private static final String CHANGELOG_TOPIC_DOC = "The changelog from the store to migrate.";

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(
          CHANGELOG_TOPIC_CONFIG,
          ConfigDef.Type.STRING,
          ConfigDef.Importance.HIGH,
          CHANGELOG_TOPIC_DOC
      );

  public ChangelogMigrationConfig(final Map<?, ?> originals) {
    super(CONFIG_DEF, originals);
  }

}
