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

package dev.responsive.kafka.internal.db;

import com.mongodb.client.model.WriteModel;
import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.db.dynamo.DynamoKVTable;
import dev.responsive.kafka.internal.db.mongo.KVDoc;
import dev.responsive.kafka.internal.db.mongo.MongoKVTable;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.db.spec.DefaultTableSpec;
import dev.responsive.kafka.internal.stores.TtlResolver;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

public class DynamoClient {

  private final ResponsiveConfig responsiveConfig;
  private final TableCache<DynamoKVTable> kvTableCache;


  public DynamoClient(
      final DynamoDbAsyncClient dynamoDB,
      final ResponsiveConfig responsiveConfig
  ) {
    this.responsiveConfig = responsiveConfig;
    kvTableCache = new TableCache<>(
        spec -> new DynamoKVTable(
            dynamoDB,
            spec.tableName()
        ));
  }

  public DynamoKVTable kvTable(
      final String name,
      final Optional<TtlResolver<?, ?>> ttlResolver
  ) throws InterruptedException, TimeoutException {
    return kvTableCache.create(
        new DefaultTableSpec(name, TablePartitioner.defaultPartitioner(), ttlResolver)
    );
  }
}
