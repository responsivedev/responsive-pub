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

package dev.responsive.kafka.api.config;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import dev.responsive.kafka.api.ResponsiveKafkaStreams;
import dev.responsive.kafka.internal.db.partitioning.Murmur3Hasher;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.streams.processor.assignment.assignors.StickyTaskAssignor;

/**
 * Configurations for {@link ResponsiveKafkaStreams}
 */
@SuppressWarnings("checkstyle:linelength")
public class ResponsiveConfig extends AbstractConfig {

  // ------------------ general Responsive configurations ---------------------

  public static final String RESPONSIVE_ORG_CONFIG = "responsive.org";
  private static final String RESPONSIVE_ORG_DOC = "The Responsive organization slug (not the organization ID).";

  public static final String RESPONSIVE_ENV_CONFIG = "responsive.env";
  private static final String RESPONSIVE_ENV_DOC = "The Responsive environment slug (not the environment ID).";

  public static final String RESPONSIVE_LICENSE_CONFIG = "responsive.license";
  private static final String RESPONSIVE_LICENSE_DOC = "The license you're using to run Responsive";

  public static final String RESPONSIVE_LICENSE_FILE_CONFIG = "responsive.license.file";
  private static final String RESPONSIVE_LICENSE_FILE_DOC
      = "A path to a file containing your license.";

  /**
   * @deprecated use the responsive.storage.backend.type config with {@link StorageBackend#NONE}
   */
  @Deprecated
  public static final String COMPATIBILITY_MODE_CONFIG = "responsive.compatibility.mode";
  private static final String COMPATIBILITY_MODE_DOC = "This configuration enables running Responsive "
      + "in compatibility mode, disabling certain features.";
  private static final CompatibilityMode COMPATIBILITY_MODE_DEFAULT = CompatibilityMode.FULL;

  public static final String CONTROLLER_ENDPOINT_CONFIG = "responsive.controller.endpoint";
  private static final String CONTROLLER_ENDPOINT_DOC = "The endpoint of the running responsive "
      + "cloud controller. If enabled, metrics will be sent to this endpoint.";

  public static final String PLATFORM_API_KEY_CONFIG = "responsive.platform.api.key";
  private static final String PLATFORM_API_KEY_DOC = "The API Key provided for Metrics access.";

  public static final String PLATFORM_API_SECRET_CONFIG = "responsive.platform.api.secret";
  private static final String PLATFORM_API_SECRET_DOC = "The Secret provided for Metrics access.";

  public static final String STORAGE_BACKEND_TYPE_CONFIG = "responsive.storage.backend.type";
  private static final String STORAGE_BACKEND_TYPE_DOC = "The storage backend";
  private static final StorageBackend STORAGE_BACKEND_TYPE_DEFAULT = StorageBackend.CASSANDRA;

  public static final String RESPONSIVE_MODE = "responsive.mode";
  public static final String RESPONSIVE_MODE_DEFAULT = ResponsiveMode.RUN.name();
  public static final String RESPONSIVE_MODE_DOC = "Determines the mode the Responsive application "
      + "runs in. When set to RUN, runs the Kafka Streams app. When set to MIGRATE, runs app"
      + " migration.";

  // ------------------ MongoDB specific configurations -----------------------

  public static final String MONGO_CONNECTION_STRING_CONFIG = "responsive.mongo.connection.string";
  private static final String MONGO_CONNECTION_STRING_DOC = "The full connection string for a MongoDB compatible server.";

  public static final String MONGO_ADDITIONAL_CONNECTION_STRING_PARAMS_CONFIG = "responsive.mongo.additional.connection.string.params";
  private static final String MONGO_ADDITIONAL_CONNECTION_STRING_PARAMS_DOC = "Additional MongoDB config options to be appended to the "
      + "connection string. ";

  public static final String MONGO_COLLECTION_SHARDING_ENABLED_CONFIG = "responsive.mongo.collection.sharding.enabled";
  private static final boolean MONGO_COLLECTION_SHARDING_ENABLED_DEFAULT = false;
  private static final String MONGO_COLLECTION_SHARDING_ENABLED_DOC = "Toggles use of sharded collections. Set "
      + "this to true when running against a sharded mongo cluster, to shard a collection across multiple mongo "
      + "replica sets.";

  public static final String MONGO_COLLECTION_SHARDING_CHUNKS_CONFIG = "responsive.mongo.collection.sharding.chunks";
  private static final int MONGO_COLLECTION_SHARDING_CHUNKS_DEFAULT = 4;
  private static final String MONGO_COLLECTION_SHARDING_CHUNKS_DOC = "For sharded collections, sets the number of "
      + "initial chunks to create the collection with.";

  public static final String MONGO_WINDOWED_KEY_TIMESTAMP_FIRST_CONFIG = "responsive.mongo.windowed.key.timestamp.first";
  private static final boolean MONGO_WINDOWED_KEY_TIMESTAMP_FIRST_DEFAULT = false;
  private static final String MONGO_WINDOWED_KEY_TIMESTAMP_FIRST_DOC = "Whether to put the window start timestamp "
      + "first in the composite windowed key format for MongoDB. This can be toggled true/false to get better "
      + "performance depending on the density of unique keys per window, and should be experimented "
      + "with for best results. However it is important to note that this cannot be changed for "
      + "an active application. Messing with this can corrupt existing state!";

  public static final String MONGO_TOMBSTONE_RETENTION_SEC_CONFIG = "responsive.mongo.tombstone.retention.seconds";
  private static final long MONGO_TOMBSTONE_RETENTION_SEC_DEFAULT = Duration.ofHours(12).toSeconds();
  private static final String MONGO_TOMBSTONE_RETENTION_SEC_DOC =
      "The duration, in milliseconds, to retain deleted records in MongoDB after they are "
          + "deleted or their TTL expires. This retention period ensures that zombie instances of "
          + "Kafka Streams are fenced from overwriting deleted or expired records. Configure this "
          + "based on your application's tolerance for potential write errors by zombie instances. "
          + "Default: 12 hours (43,200s).";

  // ------------------ RS3 specific configurations ----------------------

  public static final String RS3_HOSTNAME_CONFIG = "responsive.rs3.hostname";
  private static final String RS3_HOSTNAME_DOC = "The hostname to use when connecting to RS3.";

  public static final String RS3_PORT_CONFIG = "responsive.rs3.port";
  private static final String RS3_PORT_DOC = "The port to use when connecting to RS3.";

  public static final String RS3_LOGICAL_STORE_MAPPING_CONFIG = "responsive.rs3.logical.store.mapping";
  public static final String RS3_LOGICAL_STORE_MAPPING_DOC = "Mapping from table name to RS3 logical store ID (e.g. 'table:b1a45157-e2f0-4698-be0e-5bf3a9b8e9d1,...')";

  public static final String RS3_TLS_ENABLED_CONFIG = "responsive.rs3.tls.enabled";
  private static final String RS3_TLS_ENABLED_DOC = "Enables/disable tls for rs3 connection";

  // ------------------ ScyllaDB specific configurations ----------------------

  public static final String CASSANDRA_USERNAME_CONFIG = "responsive.cassandra.username";
  private static final String CASSANDRA_USERNAME_DOC = "The username to use when connecting to Cassandra";

  public static final String CASSANDRA_PASSWORD_CONFIG = "responsive.cassandra.password";
  private static final String CASSANDRA_PASSWORD_DOC = "The password to use when connecting to Cassandra";

  public static final String CASSANDRA_HOSTNAME_CONFIG = "responsive.cassandra.hostname";
  private static final String CASSANDRA_HOSTNAME_DOC = "The hostname to use when connecting to Cassandra";

  public static final String CASSANDRA_PORT_CONFIG = "responsive.cassandra.port";
  private static final String CASSANDRA_PORT_DOC = "The port to use when connecting to Cassandra";

  public static final String CASSANDRA_DATACENTER_CONFIG = "responsive.cassandra.datacenter";
  private static final String CASSANDRA_DATACENTER_DOC = "The datacenter to use when connecting to Cassandra";

  public static final String READ_CONSISTENCY_LEVEL_CONFIG = "responsive.cassandra.consistency.reads";
  private static final String READ_CONSISTENCY_LEVEL_DEFAULT = ConsistencyLevel.QUORUM.name();
  private static final String READ_CONSISTENCY_LEVEL_DOC = "The consistency level to set for reads";

  public static final String WRITE_CONSISTENCY_LEVEL_CONFIG = "responsive.cassandra.consistency.writes";
  private static final String WRITE_CONSISTENCY_LEVEL_DEFAULT = ConsistencyLevel.QUORUM.name();
  private static final String WRITE_CONSISTENCY_LEVEL_DOC = "The consistency level to set for writes";

  public static final String CASSANDRA_CHECK_INTERVAL_MS = "responsive.cassandra.check.interval.ms";
  private static final String CASSANDRA_CHECK_INTERVAL_MS_DOC = "The frequency at which to poll "
      + "for whether or not a remote table has been created. Mostly used to speed up integration "
      + "testing.";
  private static final long CASSANDRA_CHECK_INTERVAL_MS_DEFAULT = 1000L;

  // TODO: we should make this configurable per-store
  public static final String CASSANDRA_DESIRED_NUM_PARTITION_CONFIG = "responsive.cassandra.desired.num.partitions";
  private static final String CASSANDRA_DESIRED_NUM_PARTITIONS_DOC = "The desired number of "
      + "partitions to create in the remote store. This is a best effort target, as the actual "
      + "number of partitions will be the next multiple of the Kafka topic's number of partitions "
      + "that is greater than or equal to this number. This configuration does not apply to global "
      + "stores. A value of -1 indicates to use the number of Kafka Partitions as the remote "
      + "partitions as well.";
  public static final int CASSANDRA_DESIRED_NUM_PARTITIONS_DEFAULT = 4096;
  public static final int NO_SUBPARTITIONS = -1;

  // ------------------ metrics configurations --------------------------------

  public static final String RESPONSIVE_APPLICATION_ID_CONFIG = "responsive.application.id";
  private static final String RESPONSIVE_APPLICATION_ID_DOC = "The application ID that uniquely "
      + "identifies this application with Responsive. This defaults to the configured Kafka "
      + "Streams application.id but can be overridden in the case you have multiple applications "
      + "with the same application.id configured to run in the same responsive.tenant.id";

  public static final String METRICS_ENABLED_CONFIG = "responsive.metrics.enabled";
  private static final String METRICS_ENABLED_DOC = "Whether or not metrics should be sent to Responsive Cloud";

  // ------------------ performance related configurations --------------------

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


  // ------------------ Async processing configurations ----------------------

  public static final String ASYNC_THREAD_POOL_SIZE_CONFIG = "responsive.async.thread.pool.size";
  private static final int ASYNC_THREAD_POOL_SIZE_DEFAULT = 0;
  private static final String ASYNC_THREAD_POOL_SIZE_DOC = "The number of async processing threads to "
      + "start up for each StreamThread in this app. Setting this to 0 (the default) means async processing "
      + "will not be enabled. Setting this to a positive integer will enable async processing, but only if "
      + "there is at least one AsyncProcessor in the topology. See javadocs for AsyncProcessorSupplier for details.";

  public static final String ASYNC_MAX_EVENTS_QUEUED_PER_ASYNC_THREAD_CONFIG = "responsive.async.max.events.queued.per.async.thread";
  private static final int ASYNC_MAX_EVENTS_QUEUED_PER_ASYNC_THREAD_DEFAULT = 5;
  private static final String ASYNC_MAX_EVENTS_QUEUED_PER_ASYNC_THREAD_DOC = "The maximum number of queued events at a time for "
      + "each StreamThread as a multiple of async thread pool size. This config is used to compute the total bound on how "
      + "many events can be queued up for processing across all async processors on a StreamThread. The actual upper limit "
      + "is computed by multiplying the value of this config with the value of the responsive.async.thread.pool.size config."
      + "This puts an approximate limit on how many events each async thread will have to process in order to flush the processor, "
      + "for example during a commit or before closing the task. If you experience long flushes or StreamThreads "
      + "dropping out of the consumer group due to missing the max.poll.interval.ms, and have a large keyspace and/or many "
      + "unique keys with no updates, consider lowering this value.";

  public static final String ASYNC_MAX_EVENTS_QUEUED_PER_KEY_CONFIG = "responsive.async.max.events.queued.per.key";
  private static final int ASYNC_MAX_EVENTS_QUEUED_PER_KEY_DEFAULT = 3;
  private static final String ASYNC_MAX_EVENTS_QUEUED_PER_KEY_DOC = "The maximum number of queued events at a time for "
      + "a given key. This puts a limit on how many events for each key can be in-flight, ie awaiting or actively being"
      + "processed by the async thread pool. Since all events with the same key must be processed in offset order, this"
      + "config can be used to limit how many events will have to be processed serially in order to flush the processor, "
      + "for example during a commit or before closing the task. If you experience long flushes or StreamThreads "
      + "dropping out of the consumer group due to missing the max.poll.interval.ms, and have many updates ie many input "
      + "records with the same key, consider lowering this value.";

  public static final String ASYNC_FLUSH_INTERVAL_MS_CONFIG = "responsive.async.flush.interval.ms";
  private static final long ASYNC_FLUSH_INTERVAL_MS_DEFAULT = 5 * 1000L;
  private static final String ASYNC_FLUSH_INTERVAL_MS_DOC = "The frequency of scheduled punctuations that flush out "
      + "any events currently waiting to be scheduled or finalized. The async processing framework uses "
      + "Kafka Streams punctuators to schedule these flushes to make sure async processing can make steady "
      + "progress when input traffic to the async processors drops or goes to zero. If you expect long pauses "
      + "or gaps in the upstream input topic traffic, considering setting this to a smaller value to ensure "
      + "the async processor can continue to operate during the down periods.";


  // ------------------ WindowStore configurations ----------------------

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


  // ------------------ Misc functional overrides ----------------------
  public static final String RESTORE_OFFSET_REPAIR_ENABLED_CONFIG = "responsive.restore.offset.repair.enabled";
  public static final boolean RESTORE_OFFSET_REPAIR_ENABLED_DEFAULT = false;
  public static final String RESTORE_OFFSET_REPAIR_ENABLED_DOC = "When set to 'true', " + RESTORE_OFFSET_REPAIR_ENABLED_CONFIG
      + " will ignore OffsetOutOfRangeException and instead seek to the earliest available offset. This exception "
      + "should only happen in situations where there is truncation/retention on the changelog topic and restoring from the latest "
      + "committed offset in the remote store is no longer possible. Note that in some situations this may cause data "
      + "loss, use this configuration with caution";

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
      ).define(
          RESPONSIVE_ORG_CONFIG,
          Type.STRING,
          null,
          new ConfigDef.NonEmptyString(),
          Importance.HIGH,
          RESPONSIVE_ORG_DOC
      ).define(
          RESPONSIVE_ENV_CONFIG,
          Type.STRING,
          null,
          new ConfigDef.NonEmptyString(),
          Importance.HIGH,
          RESPONSIVE_ENV_DOC
      ).define(
          RESPONSIVE_LICENSE_CONFIG,
          Type.STRING,
          "",
          Importance.HIGH,
          RESPONSIVE_LICENSE_DOC
      ).define(
          RESPONSIVE_LICENSE_FILE_CONFIG,
          Type.STRING,
          "",
          Importance.HIGH,
          RESPONSIVE_LICENSE_FILE_DOC
      ).define(
          STORAGE_BACKEND_TYPE_CONFIG,
          Type.STRING,
          STORAGE_BACKEND_TYPE_DEFAULT.name(),
          ConfigDef.CaseInsensitiveValidString.in(StorageBackend.names()),
          Importance.HIGH,
          STORAGE_BACKEND_TYPE_DOC
      )

      // mongo connection configurations
      .define(
          MONGO_CONNECTION_STRING_CONFIG,
          Type.PASSWORD,
          null,
          new NonEmptyPassword(MONGO_CONNECTION_STRING_CONFIG),
          Importance.HIGH,
          MONGO_CONNECTION_STRING_DOC
      )

      // cassandra connection configurations
      .define(
          CASSANDRA_USERNAME_CONFIG,
          Type.STRING,
          null,
          new ConfigDef.NonEmptyString(),
          Importance.HIGH,
          CASSANDRA_USERNAME_DOC
      ).define(
          CASSANDRA_PASSWORD_CONFIG,
          Type.PASSWORD,
          null,
          new NonEmptyPassword(CASSANDRA_PASSWORD_CONFIG),
          Importance.HIGH,
          CASSANDRA_PASSWORD_DOC
      ).define(
          CASSANDRA_HOSTNAME_CONFIG,
          Type.STRING,
          null,
          new ConfigDef.NonEmptyString(),
          Importance.HIGH,
          CASSANDRA_HOSTNAME_DOC
      ).define(
          CASSANDRA_PORT_CONFIG,
          Type.INT,
          -1,
          Importance.HIGH,
          CASSANDRA_PORT_DOC
      ).define(
          CASSANDRA_DATACENTER_CONFIG,
          Type.STRING,
          null,
          new ConfigDef.NonEmptyString(),
          Importance.HIGH,
          CASSANDRA_DATACENTER_DOC
      )

      // other configs
      .define(
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
          PLATFORM_API_KEY_CONFIG,
          Type.STRING,
          "",
          Importance.HIGH,
          PLATFORM_API_KEY_DOC
      ).define(
          PLATFORM_API_SECRET_CONFIG,
          Type.PASSWORD,
          "",
          Importance.HIGH,
          PLATFORM_API_SECRET_DOC
      ).define(
          STORE_FLUSH_RECORDS_TRIGGER_CONFIG,
          Type.INT,
          STORE_FLUSH_RECORDS_TRIGGER_DEFAULT,
          Importance.MEDIUM,
          STORE_FLUSH_RECORDS_TRIGGER_DOC
      ).define(
          CASSANDRA_DESIRED_NUM_PARTITION_CONFIG,
          Type.INT,
          CASSANDRA_DESIRED_NUM_PARTITIONS_DEFAULT,
          Importance.MEDIUM,
          CASSANDRA_DESIRED_NUM_PARTITIONS_DOC
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
          CASSANDRA_CHECK_INTERVAL_MS,
          Type.LONG,
          CASSANDRA_CHECK_INTERVAL_MS_DEFAULT,
          Importance.LOW,
          CASSANDRA_CHECK_INTERVAL_MS_DOC
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
          ASYNC_THREAD_POOL_SIZE_CONFIG,
          Type.INT,
          ASYNC_THREAD_POOL_SIZE_DEFAULT,
          atLeast(0),
          Importance.LOW,
          ASYNC_THREAD_POOL_SIZE_DOC
      ).define(
          ASYNC_MAX_EVENTS_QUEUED_PER_ASYNC_THREAD_CONFIG,
          Type.INT,
          ASYNC_MAX_EVENTS_QUEUED_PER_ASYNC_THREAD_DEFAULT,
          atLeast(1),
          Importance.LOW,
          ASYNC_MAX_EVENTS_QUEUED_PER_ASYNC_THREAD_DOC
      ).define(
          ASYNC_MAX_EVENTS_QUEUED_PER_KEY_CONFIG,
          Type.INT,
          ASYNC_MAX_EVENTS_QUEUED_PER_KEY_DEFAULT,
          Importance.LOW,
          ASYNC_MAX_EVENTS_QUEUED_PER_KEY_DOC
      ).define(
          ASYNC_FLUSH_INTERVAL_MS_CONFIG,
          Type.LONG,
          ASYNC_FLUSH_INTERVAL_MS_DEFAULT,
          Importance.LOW,
          ASYNC_FLUSH_INTERVAL_MS_DOC
      ).define(
          MONGO_WINDOWED_KEY_TIMESTAMP_FIRST_CONFIG,
          Type.BOOLEAN,
          MONGO_WINDOWED_KEY_TIMESTAMP_FIRST_DEFAULT,
          Importance.LOW,
          MONGO_WINDOWED_KEY_TIMESTAMP_FIRST_DOC
      ).define(
          MONGO_ADDITIONAL_CONNECTION_STRING_PARAMS_CONFIG,
          Type.STRING,
          "",
          Importance.LOW,
          MONGO_ADDITIONAL_CONNECTION_STRING_PARAMS_DOC
      ).define(
          MONGO_TOMBSTONE_RETENTION_SEC_CONFIG,
          Type.LONG,
          MONGO_TOMBSTONE_RETENTION_SEC_DEFAULT,
          Importance.LOW,
          MONGO_TOMBSTONE_RETENTION_SEC_DOC
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
      ).define(
          RESTORE_OFFSET_REPAIR_ENABLED_CONFIG,
          Type.BOOLEAN,
          RESTORE_OFFSET_REPAIR_ENABLED_DEFAULT,
          Importance.LOW,
          RESTORE_OFFSET_REPAIR_ENABLED_DOC
      ).define(
          RS3_HOSTNAME_CONFIG,
          Type.STRING,
          "",
          Importance.MEDIUM,
          RS3_HOSTNAME_DOC
      ).define(
          RS3_PORT_CONFIG,
          Type.INT,
          50051,
          Importance.MEDIUM,
          RS3_PORT_DOC
      ).define(
          RS3_LOGICAL_STORE_MAPPING_CONFIG,
          Type.STRING,
          "",
          Importance.HIGH,
          RS3_LOGICAL_STORE_MAPPING_DOC
      ).define(
          RS3_TLS_ENABLED_CONFIG,
          Type.BOOLEAN,
          true,
          Importance.MEDIUM,
          RS3_TLS_ENABLED_DOC
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


  public Map<String, String> getMap(String config) {
    String configValue = getString(config);
    if (configValue == null) {
      throw new ConfigException("No value found for config " + config);
    }

    if (configValue.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, String> map = new HashMap<>();
    for (String mapping : configValue.split("\\s*,\\s*")) {
      int lio = mapping.lastIndexOf(":");
      String key = mapping.substring(0, lio).trim();
      String value = mapping.substring(lio + 1).trim();
      map.put(key, value);
    }
    return map;
  }
}
