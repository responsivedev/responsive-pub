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

package dev.responsive.kafka.api.config;

import static org.apache.kafka.common.config.ConfigDef.Range.between;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.internal.db.partitioning.Murmur3Hasher;
import java.time.Duration;
import java.util.Arrays;
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
 * Configurations for {@link ResponsiveKafkaStreams}
 */
@SuppressWarnings("checkstyle:linelength")
public class ResponsiveConfig extends AbstractConfig {

  // ------------------ connection configurations -----------------------------

  public static final String COMPATIBILITY_MODE_CONFIG = "responsive.compatibility.mode";
  private static final String COMPATIBILITY_MODE_DOC = "This configuration enables running Responsive "
      + "in compatibility mode, disabling certain features.";
  private static final CompatibilityMode COMPATIBILITY_MODE_DEFAULT = CompatibilityMode.FULL;

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

  public static final String STORAGE_BACKEND_TYPE_CONFIG = "responsive.storage.backend.type";
  private static final String STORAGE_BACKEND_TYPE_DOC = "The storage backend";
  private static final StorageBackend STORAGE_BACKEND_TYPE_DEFAULT = StorageBackend.CASSANDRA;

  // ------------------ metrics configurations --------------------------------

  public static final String RESPONSIVE_APPLICATION_ID_CONFIG = "responsive.application.id";
  private static final String RESPONSIVE_APPLICATION_ID_DOC = "The application ID that uniquely "
      + "identifies this application with Responsive. This defaults to the configured Kafka "
      + "Streams application.id but can be overridden in the case you have multiple applications "
      + "with the same application.id configured to run in the same responsive.tenant.id";

  public static final String METRICS_ENABLED_CONFIG = "responsive.metrics.enabled";
  private static final String METRICS_ENABLED_DOC = "Whether or not metrics should be sent to Responsive Cloud";

  public static final String CONTROLLER_ENDPOINT_CONFIG = "responsive.controller.endpoint";
  private static final String CONTROLLER_ENDPOINT_DOC = "The endpoint of the running responsive "
      + "cloud controller. If enabled, metrics will be sent to this endpoint.";

  // TODO(agavra): we should consolidate API keys, but for now it's OK to use different ones
  public static final String METRICS_API_KEY_CONFIG = "responsive.metrics.api.key";
  private static final String METRICS_API_KEY_DOC = "The API Key provided for Metrics access.";

  public static final String METRICS_SECRET_CONFIG = "responsive.metrics.secret";
  private static final String METRICS_SECRET_DOC = "The Secret provided for Metrics access.";

  // ------------------ cassandra configurations ------------------------------

  public static final String READ_CONSISTENCY_LEVEL_CONFIG = "responsive.cassandra.consistency.reads";
  private static final String READ_CONSISTENCY_LEVEL_DEFAULT = ConsistencyLevel.QUORUM.name();
  private static final String READ_CONSISTENCY_LEVEL_DOC = "The consistency level to set for reads";

  public static final String WRITE_CONSISTENCY_LEVEL_CONFIG = "responsive.cassandra.consistency.writes";
  private static final String WRITE_CONSISTENCY_LEVEL_DEFAULT = ConsistencyLevel.QUORUM.name();
  private static final String WRITE_CONSISTENCY_LEVEL_DOC = "The consistency level to set for writes";

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

  public static final String STORE_FLUSH_INTERVAL_TRIGGER_JITTER_CONFIG = "responsive.store.flush.trigger.local.jitter.ms";
  public static final String STORE_FLUSH_INTERVAL_TRIGGER_JITTER_DOC = "The jitter to apply to the flush interval."
      + "For flush interval i and jitter j, the actual flush interval for a given flush will a randomly selected duration"
      + "between i-j and i+j.";
  public static final int STORE_FLUSH_INTERVAL_TRIGGER_JITTER_DEFAULT = 0;

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

  public static final String MONGO_COLLECTION_SHARDING_ENABLED_CONFIG = "responsive.mongo.collection.sharding.enabled";
  private static final boolean MONGO_COLLECTION_SHARDING_ENABLED_DEFAULT = false;
  private static final String MONGO_COLLECTION_SHARDING_ENABLED_DOC = "Toggles use of sharded collections. Set "
      + "this to true when running against a sharded mongo cluster, to shard a collection across multiple mongo "
      + "replica sets.";

  public static final String MONGO_COLLECTION_SHARDING_CHUNKS_CONFIG = "responsive.mongo.collection.sharding.chunks";
  private static final int MONGO_COLLECTION_SHARDING_CHUNKS_DEFAULT = 4;
  private static final String MONGO_COLLECTION_SHARDING_CHUNKS_DOC = "For sharded collections, sets the number of "
      + "initial chunks to create the collection with.";


  // ------------------ WindowStore configurations ----------------------

  public static final String MONGO_WINDOWED_KEY_TIMESTAMP_FIRST_CONFIG = "responsive.mongo.windowed.key.timestamp.first";
  private static final boolean MONGO_WINDOWED_KEY_TIMESTAMP_FIRST_DEFAULT = false;
  private static final String MONGO_WINDOWED_KEY_TIMESTAMP_FIRST_DOC = "Whether to put the window start timestamp "
      + "first in the composite windowed key format for MongoDB. This can be toggled true/false to get better "
      + "performance depending on the density of unique keys per window, and should be experimented "
      + "with for best results. However it is important to note that this cannot be changed for "
      + "an active application. Messing with this can corrupt existing state!";

  public static final String WINDOW_BLOOM_FILTER_COUNT_CONFIG = "responsive.window.bloom.filter.count";
  private static final int WINDOW_BLOOM_FILTER_COUNT_DEFAULT = 0;
  private static final String WINDOW_BLOOM_FILTER_COUNT_DOC = "How many of the most recent windows to "
      + "to build a bloom filter for in order to minimize unnecessary negative remote lookups. Enable bloom "
      + "filters for windowed aggregations by setting this to a value of 1 or higher (multiple bloom filters "
      + "not yet supported. Only applies to hopping and tumbling windowed aggregations. "
      + "If enabled, we highly recommend configuring the expected number of keys per window per partition "
      + "with the responsive.window.bloom.filter.expected.keys property.";

  public static final String WINDOW_BLOOM_FILTER_EXPECTED_KEYS_CONFIG = "responsive.window.bloom.filter.expected.keys";
  private static final long WINDOW_BLOOM_FILTER_EXPECTED_KEYS_DEFAULT = 1_000L;
  private static final String WINDOW_BLOOM_FILTER_EXPECTED_KEYS_DOC = "Expected number of elements for "
      + "the WindowStore bloom filters, or the approximate number of unique keys per partition that "
      + "will be present in the most recent window of a hopping or tumbling windowed aggregation. "
      + "For loads that vary over time, use the maximum keys per partition that may appear in a window. "
      + "An overestimate will use slightly more memory, but an underestimate will significantly degrade "
      + "the bloom filter's performance";

  public static final String WINDOW_BLOOM_FILTER_FPP_CONFIG = "responsive.window.bloom.filter.fpp";
  private static final double WINDOW_BLOOM_FILTER_FPP_DEFAULT = 0.03d;
  private static final String WINDOW_BLOOM_FILTER_FPP_DOC = "A double representing the desired false "
      + "positive percentage for the WindowStore bloom filters. A smaller value means fewer unnecessary "
      + "lookups but requires more heap memory";


  public static final String RESPONSIVE_MODE = "responsive.mode";
  public static final String RESPONSIVE_MODE_DEFAULT = ResponsiveMode.RUN.name();
  public static final String RESPONSIVE_MODE_DOC = "Determines the mode the Responsive application "
      + "runs in. When set to RUN, runs the Kafka Streams app. When set to MIGRATE, runs app"
      + " migration.";

  // ------------------ StreamsConfig overrides ----------------------

  // These configuration values are required by Responsive, and a ConfigException will
  // be thrown if they are overridden to anything else
  public static final int NUM_STANDBYS_OVERRIDE = 0;
  public static final String TASK_ASSIGNOR_CLASS_OVERRIDE = StickyTaskAssignor.class.getName();

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(
          COMPATIBILITY_MODE_CONFIG,
          Type.STRING,
          COMPATIBILITY_MODE_DEFAULT.name(),
          ConfigDef.CaseInsensitiveValidString.in(CompatibilityMode.names()),
          Importance.MEDIUM,
          COMPATIBILITY_MODE_DOC
      )
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
          STORAGE_BACKEND_TYPE_CONFIG,
          Type.STRING,
          STORAGE_BACKEND_TYPE_DEFAULT.name(),
          ConfigDef.CaseInsensitiveValidString.in(StorageBackend.names()),
          Importance.HIGH,
          STORAGE_BACKEND_TYPE_DOC
      ).define(
          RESPONSIVE_APPLICATION_ID_CONFIG,
          Type.STRING,
          "",
          Importance.MEDIUM,
          RESPONSIVE_APPLICATION_ID_DOC
      ).define(
          METRICS_ENABLED_CONFIG,
          Type.BOOLEAN,
          false,
          Importance.MEDIUM,
          METRICS_ENABLED_DOC
      ).define(
          CONTROLLER_ENDPOINT_CONFIG,
          Type.STRING,
          "",
          Importance.HIGH,
          CONTROLLER_ENDPOINT_DOC
      ). define(
          METRICS_API_KEY_CONFIG,
          Type.STRING,
          "",
          Importance.HIGH,
          METRICS_API_KEY_DOC
      ).define(
          METRICS_SECRET_CONFIG,
          Type.PASSWORD,
          "",
          Importance.HIGH,
          METRICS_SECRET_DOC
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
          STORE_FLUSH_INTERVAL_TRIGGER_JITTER_CONFIG,
          Type.INT,
          STORE_FLUSH_INTERVAL_TRIGGER_JITTER_DEFAULT,
          Importance.LOW,
          STORE_FLUSH_INTERVAL_TRIGGER_JITTER_DOC
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
      ).define(
          MONGO_COLLECTION_SHARDING_ENABLED_CONFIG,
          Type.BOOLEAN,
          MONGO_COLLECTION_SHARDING_ENABLED_DEFAULT,
          Importance.LOW,
          MONGO_COLLECTION_SHARDING_ENABLED_DOC
      ).define(
          MONGO_COLLECTION_SHARDING_CHUNKS_CONFIG,
          Type.INT,
          MONGO_COLLECTION_SHARDING_CHUNKS_DEFAULT,
          Importance.LOW,
          MONGO_COLLECTION_SHARDING_CHUNKS_DOC
      ).define(
          MONGO_WINDOWED_KEY_TIMESTAMP_FIRST_CONFIG,
          Type.BOOLEAN,
          MONGO_WINDOWED_KEY_TIMESTAMP_FIRST_DEFAULT,
          Importance.LOW,
          MONGO_WINDOWED_KEY_TIMESTAMP_FIRST_DOC
      ).define(
          WINDOW_BLOOM_FILTER_COUNT_CONFIG,
          Type.INT,
          WINDOW_BLOOM_FILTER_COUNT_DEFAULT,
          between(0, 1),
          Importance.LOW,
          WINDOW_BLOOM_FILTER_COUNT_DOC
      ).define(
          WINDOW_BLOOM_FILTER_EXPECTED_KEYS_CONFIG,
          Type.LONG,
          WINDOW_BLOOM_FILTER_EXPECTED_KEYS_DEFAULT,
          Importance.LOW,
          WINDOW_BLOOM_FILTER_EXPECTED_KEYS_DOC
      ).define(
          WINDOW_BLOOM_FILTER_FPP_CONFIG,
          Type.DOUBLE,
          WINDOW_BLOOM_FILTER_FPP_DEFAULT,
          Importance.LOW,
          WINDOW_BLOOM_FILTER_FPP_DOC
      ).define(
          READ_CONSISTENCY_LEVEL_CONFIG,
          Type.STRING,
          READ_CONSISTENCY_LEVEL_DEFAULT,
          ConfigDef.CaseInsensitiveValidString.in(
              Arrays.stream(DefaultConsistencyLevel.values())
                  .map(Enum::name)
                  .toArray(String[]::new)),
          Importance.MEDIUM,
          READ_CONSISTENCY_LEVEL_DOC
      ).define(
          WRITE_CONSISTENCY_LEVEL_CONFIG,
          Type.STRING,
          WRITE_CONSISTENCY_LEVEL_DEFAULT,
          ConfigDef.CaseInsensitiveValidString.in(
              Arrays.stream(DefaultConsistencyLevel.values())
                  .map(Enum::name)
                  .toArray(String[]::new)),
          Importance.MEDIUM,
          WRITE_CONSISTENCY_LEVEL_DOC
      ).define(
          RESPONSIVE_MODE,
          Type.STRING,
          RESPONSIVE_MODE_DEFAULT,
          ConfigDef.CaseInsensitiveValidString.in(
              Arrays.stream(ResponsiveMode.values())
                  .map(Enum::name)
                  .toArray(String[]::new)
          ),
          Importance.LOW,
          RESPONSIVE_MODE_DOC
      );

  /**
   * This should generally be used over the {@link #loggedConfig(Map)} override in all
   * cases outside the initial {@link ResponsiveKafkaStreams} constructor.
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
