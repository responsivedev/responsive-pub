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

package dev.responsive.kafka.config;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.streams.processor.internals.assignment.StickyTaskAssignor;

/**
 * Configurations for {@link dev.responsive.kafka.api.ResponsiveDriver}
 */
public class ResponsiveDriverConfig extends AbstractConfig {

  // ------------------ connection configurations -----------------------------

  public static final String STORAGE_HOSTNAME_CONFIG = "responsive.storage.hostname";
  private static final String STORAGE_HOSTNAME_DOC = "The hostname of the storage server.";

  public static final String STORAGE_PORT_CONFIG = "responsive.storage.port";
  private static final String STORAGE_PORT_DOC = "The port of the storage server.";

  public static final String STORAGE_DATACENTER_CONFIG = "responsive.storage.datacenter";
  public static final String STORAGE_DATACENTER_DOC = "The datacenter for the storage server";

  public static final String CONNECTION_BUNDLE_CONFIG = "responsive.connection.bundle";
  private static final String CONNECTION_BUNDLE_DOC = "Path to the configuration bundle for "
      + "connecting to Responsive cloud. Either this or " + STORAGE_HOSTNAME_CONFIG
      + ", " + STORAGE_PORT_CONFIG + " and " + STORAGE_DATACENTER_CONFIG + " must be set.";

  public static final String TENANT_ID_CONFIG = "responsive.tenant.id";
  private static final String TENANT_ID_DOC = "The tenant ID provided by Responsive for "
      + "resource isolation.";

  public static final String CLIENT_ID_CONFIG = "responsive.client.id";
  private static final String CLIENT_ID_DOC = "The client ID for authenticated access";

  public static final String CLIENT_SECRET_CONFIG = "responsive.client.secret";
  private static final String CLIENT_SECRET_DOC = "The client secret for authenticated access";

  // ------------------ request configurations --------------------------------

  public static final String REQUEST_TIMEOUT_MS_CONFIG = "responsive.request.timeout.ms";
  private static final String REQUEST_TIMEOUT_MS_DOC = "The timeout for making requests to the "
      + "responsive server. This applies both to metadata requests and query execution.";
  private static final long REQUEST_TIMEOUT_MS_DEFAULT = 5000L;

  // ------------------- other configurations ---------------------------------
  public static final String PARTITION_EXPLODE_FACTOR_CONFIG = "responsive.partition"
      + ".explode.factor";
  public static final String PARTITION_EXPLODE_FACTOR_DOC = "Explodes kafka partitions "
      + "into remote partition counts";
  public static final int PARTITION_EXPLODE_FACTOR_DEFAULT = 31;

  public static final String INTERNAL_PARTITIONER = "__internal.responsive.partitioner__";

  // ------------------ required StreamsConfig overrides ----------------------

  public static final int NUM_STANDBYS_OVERRIDE = 0;
  public static final String TASK_ASSIGNOR_CLASS_OVERRIDE = StickyTaskAssignor.class.getName();

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(
          STORAGE_HOSTNAME_CONFIG,
          Type.STRING,
          null,
          new ConfigDef.NonEmptyString(),
          Importance.HIGH,
          STORAGE_HOSTNAME_DOC
      ).define(
          STORAGE_PORT_CONFIG,
          Type.INT,
          -1,
          Importance.HIGH,
          STORAGE_PORT_DOC
      ).define(
          STORAGE_DATACENTER_CONFIG,
          Type.STRING,
          null,
          new ConfigDef.NonEmptyString(),
          Importance.HIGH,
          STORAGE_DATACENTER_DOC
      ).define(
          CONNECTION_BUNDLE_CONFIG,
          Type.STRING,
          null,
          new ConfigDef.NonEmptyString(),
          Importance.HIGH,
          CONNECTION_BUNDLE_DOC
      ).define(
          TENANT_ID_CONFIG,
          Type.STRING,
          Importance.HIGH,
          TENANT_ID_DOC
      ).define(
          CLIENT_ID_CONFIG,
          Type.STRING,
          null,
          new ConfigDef.NonEmptyString(),
          Importance.HIGH,
          CLIENT_ID_DOC
      ).define(
          CLIENT_SECRET_CONFIG,
          Type.PASSWORD,
          null,
          new NonEmptyPassword(CLIENT_SECRET_CONFIG),
          Importance.HIGH,
          CLIENT_SECRET_DOC
      ).define(
          REQUEST_TIMEOUT_MS_CONFIG,
          Type.LONG,
          REQUEST_TIMEOUT_MS_DEFAULT,
          Importance.MEDIUM,
          REQUEST_TIMEOUT_MS_DOC
      ).define(
          PARTITION_EXPLODE_FACTOR_CONFIG,
          Type.INT,
          PARTITION_EXPLODE_FACTOR_DEFAULT,
          Importance.MEDIUM,
          PARTITION_EXPLODE_FACTOR_DOC
      );

  private static class NonEmptyPassword implements Validator {
    private final String passwordConfig;

    NonEmptyPassword(final String passwordConfig) {
      this.passwordConfig = passwordConfig;
    }

    @Override
    public void ensureValid(String name, Object o) {
      Password p = (Password) o;
      if (p != null && p.value().isEmpty()) {
        throw new ConfigException(name, o, passwordConfig + " must be non-empty");
      }
    }

    @Override
    public String toString() {
      return "non-empty password";
    }
  }

  public ResponsiveDriverConfig(final Map<?, ?> originals) {
    super(CONFIG_DEF, originals);
  }
}