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

package dev.responsive.kafka.internal.db.spec;

import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import dev.responsive.kafka.internal.db.RemoteTable;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.stores.TtlResolver;
import java.util.Optional;

/**
 * Defines the table specifications for a {@link RemoteTable} that are
 * independent of the schema of the table.
 */
public interface RemoteTableSpec {

  String tableName();

  TablePartitioner<?, ?> partitioner();

  Optional<TtlResolver<?, ?>> ttlResolver();

  CreateTableWithOptions applyDefaultOptions(final CreateTableWithOptions base);

}
