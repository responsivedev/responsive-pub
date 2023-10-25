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

package dev.responsive.kafka.internal.db.spec;

import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import dev.responsive.kafka.internal.db.RemoteTable;
import dev.responsive.kafka.internal.db.TableOperations;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import java.util.EnumSet;

/**
 * Defines the table specifications for a {@link RemoteTable} that are
 * independent of the schema of the table.
 */
public interface CassandraTableSpec {

  String tableName();

  TablePartitioner<?, ?> partitioner();

  /**
   * @return the set of operations that are not supported by this table
   */
  EnumSet<TableOperations> restrictedOperations();

  CreateTableWithOptions applyOptions(final CreateTableWithOptions base);

}
