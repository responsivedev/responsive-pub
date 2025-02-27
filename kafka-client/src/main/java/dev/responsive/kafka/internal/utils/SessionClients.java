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

package dev.responsive.kafka.internal.utils;

import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.internal.db.CassandraClient;
import dev.responsive.kafka.internal.db.mongo.ResponsiveMongoClient;
import dev.responsive.kafka.internal.db.rs3.RS3TableFactory;
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
  private final Optional<RS3TableFactory> rs3TableFactory;
  private final StorageBackend storageBackend;
  private final Admin admin;

  // These are effectively final, but have to be inserted after the SessionClients is
  // created (see the comment above #initialize for more details)
  private ResponsiveMetrics metrics;
  private ResponsiveRestoreListener restoreListener;

  public SessionClients(
      final Optional<ResponsiveMongoClient> mongoClient,
      final Optional<CassandraClient> cassandraClient,
      final Optional<RS3TableFactory> rs3TableFactory,
      final StorageBackend storageBackend,
      final Admin admin
  ) {
    this.mongoClient = mongoClient;
    this.cassandraClient = cassandraClient;
    this.rs3TableFactory = rs3TableFactory;
    this.storageBackend = storageBackend;
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
    return storageBackend;
  }

  public RS3TableFactory rs3TableFactory() {
    if (storageBackend != StorageBackend.RS3) {
      final IllegalStateException fatalException =
          new IllegalStateException("Storage backend was " + storageBackend + " not rs3");
      LOG.error(fatalException.getMessage(), fatalException);
      throw fatalException;
    }

    if (rs3TableFactory.isEmpty()) {
      final IllegalStateException fatalException =
          new IllegalStateException("rs3 table factory was missing");
      LOG.error(fatalException.getMessage(), fatalException);
      throw fatalException;
    }
    return rs3TableFactory.get();
  }

  public ResponsiveMongoClient mongoClient() {
    if (storageBackend != StorageBackend.MONGO_DB) {
      final IllegalStateException fatalException =
          new IllegalStateException("Storage backend was " + storageBackend + " not mongoDB");
      LOG.error(fatalException.getMessage(), fatalException);
      throw fatalException;
    }

    if (mongoClient.isEmpty()) {
      final IllegalStateException fatalException =
          new IllegalStateException("MongoDB client was missing");
      LOG.error(fatalException.getMessage(), fatalException);
      throw fatalException;
    }

    return mongoClient.get();
  }

  public CassandraClient cassandraClient() {
    if (storageBackend != StorageBackend.CASSANDRA) {
      final IllegalStateException fatalException =
          new IllegalStateException("Storage backend was " + storageBackend + " not cassandra");
      LOG.error(fatalException.getMessage(), fatalException);
      throw fatalException;
    }

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
    rs3TableFactory.ifPresent(RS3TableFactory::close);
    admin.close();

    if (restoreListener != null) {
      restoreListener.close();
    }
  }
}
