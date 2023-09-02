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

import dev.responsive.db.partitioning.Hasher;
import dev.responsive.db.partitioning.Murmur3Hasher;
import dev.responsive.db.partitioning.SubPartitioner;
import dev.responsive.utils.TableName;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.streams.processor.internals.assignment.StickyTaskAssignor;

/**
 * Configurations for {@link dev.responsive.kafka.api.ResponsiveKafkaStreams}
 */
@SuppressWarnings("checkstyle:linelength")
public class ResponsiveConfig extends AbstractConfig {

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

  public static final String REMOTE_TABLE_CHECK_INTERVAL_MS_CONFIG = "responsive.remote.table.check.interval.ms";
  private static final String REMOTE_TABLE_CHECK_INTERVAL_MS_DOC = "The frequency at which to poll "
      + "for whether or not a remote table has been created. Mostly used to speed up integration "
      + "testing.";
  private static final long REMOTE_TABLE_CHECK_INTERVAL_MS_DEFAULT = 1000L;

  // ------------------ performance related configurations --------------------

  // TODO: we should make this configurable per-store
  public static final String STORAGE_DESIRED_NUM_PARTITION_CONFIG = "responsive.storage.desired.num.partitions";
  private static final String STORAGE_DESIRED_NUM_PARTITIONS_DOC = "The desired number of "
      + "partitions to create in the remote store. This is a best effort target, as the actual "
      + "number of partitions will be the next multiple of the Kafka topic's number of partitions "
      + "that is greater than or equal to this number. This configuration does not apply to global "
      + "stores. A value of -1 indicates to use the number of Kafka Partitions as the remote "
      + "partitions as well.";
  public static final int STORAGE_DESIRED_NUM_PARTITIONS_DEFAULT = 4096;
  public static final int NO_SUBPARTITIONS = -1;

  // TODO: we should have another config that's applied globally, that sets a size bound on
  //       the total amount of buffered data. That config can be used to keep a bound on
  //       memory usage and restore times.

  public static final String STORE_FLUSH_RECORDS_TRIGGER_CONFIG = "responsive.store.flush.trigger.local.records";
  public static final String STORE_FLUSH_RECORDS_TRIGGER_DOC = "The number of records to"
      + " accumulate in each store before flushing to remote";
  public static final int STORE_FLUSH_RECORDS_TRIGGER_DEFAULT = Integer.MAX_VALUE;

  public static final String STORE_FLUSH_BYTES_TRIGGER_CONFIG = "responsive.store.flush.trigger.local.bytes";
  public static final String STORE_FLUSH_BYTES_TRIGGER_DOC = "The size in bytes of buffered"
      + "records to accumulate in each store before flushing to remote";
  public static final Long STORE_FLUSH_BYTES_TRIGGER_DEFAULT = Long.MAX_VALUE;

  public static final String STORE_FLUSH_INTERVAL_TRIGGER_MS_CONFIG = "responsive.store.flush.trigger.local.interval.ms";
  public static final String STORE_FLUSH_INTERVAL_TRIGGER_DOC = "The maximum time to wait "
      + "between consecutive flushes of a given store. Note that this is only evaluated at "
      + "commit time.";
  public static final long STORE_FLUSH_INTERVAL_TRIGGER_DEFAULT
      = Duration.ofSeconds(30).toMillis();

  // TODO: consider if we want this as a local, global or per-store configuration
  public static final String MAX_CONCURRENT_REQUESTS_CONFIG = "responsive.max.concurrent.requests";
  private static final String MAX_CONCURRENT_REQUESTS_DOC = "The maximum number of requests"
      + "that will every be concurrently issued to the remote store. Increasing this value will "
      + "reduce the time that it takes to flush data to the remote store, but potentially increase "
      + "the latency of each individual request and any other concurrent reads from other threads. "
      + "This configuration is 'local': in practice, the number of concurrent requests from all "
      + "applications to the remote store is this number * the number of stream threads running "
      + "across all nodes.";
  // set a conservative default here, it's better to have the user experience be
  // one that doesn't error out and instead allow them to tune this up if they
  // are not getting the throughput that they need
  public static final int MAX_CONCURRENT_REQUESTS_DEFAULT = 128;

  public static final String SUBPARTITION_HASHER_CONFIG = "responsive.subpartition.hasher";
  private static final String SUBPARTITION_HASHER_DOC = "Hasher to use for sub-partitioning.";
  private static final Class<?> SUBPARTITION_HASHER_DEFAULT = Murmur3Hasher.class;

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
          STORE_FLUSH_RECORDS_TRIGGER_CONFIG,
          Type.INT,
          STORE_FLUSH_RECORDS_TRIGGER_DEFAULT,
          Importance.MEDIUM,
          STORE_FLUSH_RECORDS_TRIGGER_DOC
      ).define(
          STORAGE_DESIRED_NUM_PARTITION_CONFIG,
          Type.INT,
          STORAGE_DESIRED_NUM_PARTITIONS_DEFAULT,
          Importance.MEDIUM,
          STORAGE_DESIRED_NUM_PARTITIONS_DOC
      ).define(
          MAX_CONCURRENT_REQUESTS_CONFIG,
          Type.INT,
          MAX_CONCURRENT_REQUESTS_DEFAULT,
          Importance.MEDIUM,
          MAX_CONCURRENT_REQUESTS_DOC
      ).define(
          STORE_FLUSH_BYTES_TRIGGER_CONFIG,
          Type.LONG,
          STORE_FLUSH_BYTES_TRIGGER_DEFAULT,
          Importance.MEDIUM,
          STORE_FLUSH_BYTES_TRIGGER_DOC
      ).define(
          STORE_FLUSH_INTERVAL_TRIGGER_MS_CONFIG,
          Type.LONG,
          STORE_FLUSH_INTERVAL_TRIGGER_DEFAULT,
          Importance.MEDIUM,
          STORE_FLUSH_INTERVAL_TRIGGER_DOC
      ).define(
          SUBPARTITION_HASHER_CONFIG,
          Type.CLASS,
          SUBPARTITION_HASHER_DEFAULT,
          Importance.LOW,
          SUBPARTITION_HASHER_DOC
      ).define(
          REMOTE_TABLE_CHECK_INTERVAL_MS_CONFIG,
          Type.LONG,
          REMOTE_TABLE_CHECK_INTERVAL_MS_DEFAULT,
          Importance.LOW,
          REMOTE_TABLE_CHECK_INTERVAL_MS_DOC
      );

  /**
   * This should generally be used over the {@link #loggedConfig(Map)} override in all
   * cases outside the initial {@link dev.responsive.kafka.api.ResponsiveKafkaStreams#create}.
   * Prefer this API whenever you need to construct a {@code ResponsiveConfig} mid-application,
   * for example during state store init.
   */
  public static ResponsiveConfig responsiveConfig(final Map<?, ?> originals) {
    return new ResponsiveConfig(originals, false);
  }

  /**
   * This should generally be used only once per app to avoid excessive logging. Prefer
   * {@link #responsiveConfig(Map)} whenever you need to construct a {@code ResponsiveConfig}
   * mid-application, for example during state store init.
   */
  public static ResponsiveConfig loggedConfig(final Map<?, ?> originals) {
    return new ResponsiveConfig(originals, true);
  }

  private ResponsiveConfig(final Map<?, ?> originals, final boolean doLog) {
    super(CONFIG_DEF, originals, doLog);
  }

  public SubPartitioner getSubPartitioner(
      final Admin admin,
      final TableName name,
      final String changelogTopicName
  ) {
    // TODO(agavra): write the actual remote partition count into cassandra
    final OptionalInt actualRemoteCount = OptionalInt.empty();
    final int kafkaPartitions;
    try {
      kafkaPartitions = admin.describeTopics(List.of(changelogTopicName))
          .allTopicNames()
          .get()
          .get(changelogTopicName)
          .partitions()
          .size();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    return SubPartitioner.create(
        actualRemoteCount,
        kafkaPartitions,
        getInt(STORAGE_DESIRED_NUM_PARTITION_CONFIG),
        name,
        changelogTopicName,
        getConfiguredInstance(SUBPARTITION_HASHER_CONFIG, Hasher.class)
    );
  }

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

}
