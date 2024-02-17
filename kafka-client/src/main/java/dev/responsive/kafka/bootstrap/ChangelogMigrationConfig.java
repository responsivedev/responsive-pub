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

package dev.responsive.kafka.bootstrap;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class ChangelogMigrationConfig extends AbstractConfig {

  // ----------------- general configurations ---------------------------

  public static final String CHANGELOG_TOPIC_CONFIG = "responsive.migration.changelog.topic";
  private static final String CHANGELOG_TOPIC_DOC = "The changelog from the store to migrate.";

  public static final String TABLE_NAME_CONFIG = "responsive.migration.table.name";
  private static final String TABLE_NAME_DOC = "The table name. This should match the table "
      + "name in the original application that is being migrated.";

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(
          CHANGELOG_TOPIC_CONFIG,
          ConfigDef.Type.STRING,
          ConfigDef.Importance.HIGH,
          CHANGELOG_TOPIC_DOC
      ).define(
          TABLE_NAME_CONFIG,
          ConfigDef.Type.STRING,
          ConfigDef.Importance.HIGH,
          TABLE_NAME_DOC
      );

  public ChangelogMigrationConfig(final Map<?, ?> originals) {
    super(CONFIG_DEF, originals);
  }

}
