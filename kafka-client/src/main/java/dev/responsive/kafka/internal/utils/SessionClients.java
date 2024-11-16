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

package dev.responsive.kafka.internal.utils;

import dev.responsive.kafka.api.config.CompatibilityMode;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.mongo.ResponsiveMongoClient;
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
  private final boolean inMemory;
  private final Admin admin;

  // These are effectively final, but have to be inserted after the SessionClients is
  // created (see the comment above #initialize for more details)
  private ResponsiveMetrics metrics;
  private ResponsiveRestoreListener restoreListener;

  public SessionClients(
      final Optional<ResponsiveMongoClient> mongoClient,
      final Optional<CassandraClient> cassandraClient,
      final boolean inMemory,
      final Admin admin
  ) {
    this.mongoClient = mongoClient;
    this.cassandraClient = cassandraClient;
    this.inMemory = inMemory;
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
    } else if (inMemory) {
      return StorageBackend.IN_MEMORY;
    } else {
      throw new IllegalArgumentException("Invalid Shared Clients Configuration. "
          + "If you have configured " + ResponsiveConfig.COMPATIBILITY_MODE_CONFIG
          + "=" + CompatibilityMode.METRICS_ONLY + " you cannot use Responsive storage. "
          + "See https://docs.responsive.dev/getting-started/quickstart for a how-to guide for "
          + "getting started with Responsive stores.");
    }
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
