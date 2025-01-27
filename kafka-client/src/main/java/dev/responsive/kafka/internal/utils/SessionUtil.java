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

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_CONSISTENCY;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_THROTTLER_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.RETRY_POLICY_CLASS;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.internal.core.session.throttling.ConcurrencyLimitingRequestThrottler;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import dev.responsive.kafka.internal.db.ResponsiveRetryPolicy;
import dev.responsive.kafka.internal.db.mongo.MongoTelemetryListener;
import dev.responsive.kafka.internal.metrics.CassandraMetricsFactory;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import java.net.InetSocketAddress;
import javax.annotation.Nullable;
import org.bson.Document;

/**
 * This is a utility to help creating a session to connect to the remote
 * Responsive store.
 */
public final class SessionUtil {
  private SessionUtil() {
  }

  public static CqlSession connect(
      final InetSocketAddress address,
      final String datacenter,
      final String keyspace,
      @Nullable final String username,
      @Nullable final String password,
      final int maxConcurrentRequests,
      @Nullable ResponsiveMetrics metrics

  ) {
    final CqlSessionBuilder sessionBuilder = CqlSession.builder()
        .withLocalDatacenter(datacenter)
        .addContactPoint(address);

    if (metrics != null) {
      sessionBuilder.withMetricRegistry(metrics);
    }

    if (username != null && password != null) {
      sessionBuilder.withAuthCredentials(username, password);
    } else if (username == null ^ password == null) {
      throw new IllegalArgumentException(
          "Must specify both or neither Cassandra username and password.");
    }

    return sessionBuilder
        .withConfigLoader(DriverConfigLoader
            .programmaticBuilder()
            .withString(DefaultDriverOption.METRICS_FACTORY_CLASS,
                        CassandraMetricsFactory.class.getCanonicalName())
            .withLong(REQUEST_TIMEOUT, 5000)
            .withClass(RETRY_POLICY_CLASS, ResponsiveRetryPolicy.class)
            .withClass(REQUEST_THROTTLER_CLASS, ConcurrencyLimitingRequestThrottler.class)
            // we just set this to MAX_VALUE as it will be implicitly limited by the
            // number of stream threads * the max number of records being flushed -
            // we do not want to throw an exception if the queue is full as this will
            // cause a rebalance
            .withInt(REQUEST_THROTTLER_MAX_QUEUE_SIZE, Integer.MAX_VALUE)
            .withInt(REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS, maxConcurrentRequests)
            .withString(REQUEST_CONSISTENCY, ConsistencyLevel.QUORUM.name())
            .build())
        .withKeyspace(keyspace)
        .build();
  }

  public static MongoClient connect(
      final String connectionString,
      final String additionalParams,
      @Nullable final ResponsiveMetrics metrics
  ) {
    final String connectionStringWithParams = additionalParams.equals("")
        ? connectionString
        : connectionString + "/?" + additionalParams;

    final MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString(connectionString))
        .readConcern(ReadConcern.MAJORITY)
        .writeConcern(WriteConcern.MAJORITY);

    if (metrics != null) {
      settingsBuilder.addCommandListener(new MongoTelemetryListener(metrics));
    }

    MongoClientSettings settings = settingsBuilder.build();

    // Create a new client and connect to the server
    MongoClient mongoClient = MongoClients.create(settings);
    try {
      // Send a ping to confirm a successful connection
      MongoDatabase database = mongoClient.getDatabase("admin");
      database.runCommand(new Document("ping", 1));
      System.out.println("Pinged your deployment. You successfully connected to MongoDB!");
      return mongoClient;
    } catch (MongoException e) {
      throw new RuntimeException(e);
    }
  }
}
