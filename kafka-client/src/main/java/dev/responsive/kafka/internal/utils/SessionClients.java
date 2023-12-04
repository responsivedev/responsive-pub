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

package dev.responsive.kafka.internal.utils;

import dev.responsive.kafka.api.config.CompatibilityMode;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.mongo.ResponsiveMongoClient;
import dev.responsive.kafka.internal.db.otterpocket.KVOtterPocketClient;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.metrics.ResponsiveRestoreListener;
import java.util.Optional;
import org.apache.kafka.clients.admin.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic container class for session clients and other shared resources that should only
 * be closed when the app itself is shutdown
 */
public class SessionClients {
  private static final Logger LOG = LoggerFactory.getLogger(SessionClients.class);

  private final Optional<ResponsiveMongoClient> mongoClient;
  private final Optional<CassandraClient> cassandraClient;
  private final Optional<KVOtterPocketClient> otterPocketClient;
  private final Admin admin;

  // These are effectively final, but have to be inserted after the SessionClients is
  // created (see the comment above #initialize for more details)
  private ResponsiveMetrics metrics;
  private ResponsiveRestoreListener restoreListener;

  public SessionClients(
      final Optional<ResponsiveMongoClient> mongoClient,
      final Optional<CassandraClient> cassandraClient,
      final Optional<KVOtterPocketClient> otterPocketClient,
      final Admin admin
  ) {
    this.mongoClient = mongoClient;
    this.cassandraClient = cassandraClient;
    this.otterPocketClient = otterPocketClient;
    this.admin = admin;
  }

  // We unfortunately can't pass these in when creating the SessionClients
  // as we are forced to insert the SessionClients into the StreamsConfig passed in to
  // the KafkaStreams constructor, while the metrics & restore listener depend
  // on some things created during/after the KafkaStreams is. These should always
  // be registered before an application is considered fully initialized
  public void initialize(
      final ResponsiveMetrics metrics,
      final ResponsiveRestoreListener restoreListener
  ) {
    this.metrics = metrics;
    this.restoreListener = restoreListener;
  }

  public StorageBackend storageBackend() {
    if (mongoClient.isPresent()) {
      return StorageBackend.MONGO_DB;
    } else if (cassandraClient.isPresent()) {
      return StorageBackend.CASSANDRA;
    } else if (otterPocketClient.isPresent()) {
      return StorageBackend.OTTER_POCKET;
    } else {
      throw new IllegalArgumentException("Invalid Shared Clients Configuration. "
          + "If you have configured " + ResponsiveConfig.COMPATIBILITY_MODE_CONFIG
          + "=" + CompatibilityMode.METRICS_ONLY + " you cannot use Responsive storage. "
          + "See https://docs.responsive.dev/getting-started/quickstart for a how-to guide for "
          + "getting started with Responsive stores.");
    }
  }

  public KVOtterPocketClient otterPocketCLient() {
    if (otterPocketClient.isEmpty()) {
      final IllegalStateException fatalException =
          new IllegalStateException("OtterPocket client was missing");
      LOG.error(fatalException.getMessage(), fatalException);
      throw fatalException;
    }

    return otterPocketClient.get();
  }

  public ResponsiveMongoClient mongoClient() {
    if (mongoClient.isEmpty()) {
      final IllegalStateException fatalException =
          new IllegalStateException("MongoDB client was missing");
      LOG.error(fatalException.getMessage(), fatalException);
      throw fatalException;
    }

    return mongoClient.get();
  }

  public CassandraClient cassandraClient() {
    if (cassandraClient.isEmpty()) {
      final IllegalStateException fatalException =
          new IllegalStateException("Cassandra client was missing");
      LOG.error(fatalException.getMessage(), fatalException);
      throw fatalException;
    }

    return cassandraClient.get();
  }

  public Admin admin() {
    return admin;
  }

  public ResponsiveRestoreListener restoreListener() {
    return restoreListener;
  }

  public ResponsiveMetrics metrics() {
    return metrics;
  }

  public void closeAll() {
    cassandraClient.ifPresent(CassandraClient::shutdown);
    mongoClient.ifPresent(ResponsiveMongoClient::close);
    admin.close();

    if (restoreListener != null) {
      restoreListener.close();
    }
  }
}
