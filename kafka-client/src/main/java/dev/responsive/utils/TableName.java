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

package dev.responsive.utils;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * {@code TableName} represents the name for a table and all variations
 * of it - some characters that are valid for Kafka names are not valid
 * as Cassandra table names.
 */
public class TableName {

  // cassandra names are case-insensitive and can only contain
  // alphanumeric and underscore characters
  private static final Pattern INVALID_CASSANDRA_CHARS =
      Pattern.compile("[^a-zA-Z0-9_]");

  private final String kafkaName;
  private final String cassandraName;

  public TableName(final String kafkaName) {
    this.kafkaName = kafkaName;
    final var escaped = kafkaName.replaceAll("_", "__"); // escape existing underscores
    cassandraName = INVALID_CASSANDRA_CHARS
        .matcher(escaped)
        .replaceAll("_")
        .toLowerCase();
  }

  public String kafkaName() {
    return kafkaName;
  }

  public String cassandraName() {
    return cassandraName;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TableName tableName = (TableName) o;
    return Objects.equals(kafkaName, tableName.kafkaName)
        && Objects.equals(cassandraName, tableName.cassandraName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kafkaName, cassandraName);
  }

  @Override
  public String toString() {
    return kafkaName + "(" + cassandraName + ")";
  }
}
