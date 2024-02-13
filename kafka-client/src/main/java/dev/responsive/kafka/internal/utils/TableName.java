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

import java.util.Objects;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code TableName} represents the name for a table and all variations
 * of it - some characters that are valid for Kafka names are not valid
 * as table names.
 */
public class TableName {

  private static final Logger LOG = LoggerFactory.getLogger(TableName.class);

  private static final int MAXIMUM_LENGTH = 65;

  // table names are case-insensitive and should only contain
  // alphanumeric and underscore characters
  private static final Pattern INVALID_CHARS =
      Pattern.compile("[^a-zA-Z0-9_]");

  private final String kafkaName;
  private final String tableName;

  public TableName(final String kafkaName) {
    this.kafkaName = kafkaName;
    final var escaped = kafkaName.replaceAll("_", "__"); // escape existing underscores
    tableName = INVALID_CHARS
        .matcher(escaped)
        .replaceAll("_")
        .toLowerCase();

    if (tableName.length() > MAXIMUM_LENGTH) {
      LOG.error("Invalid table name for state store {}, must be no more than {} characters long"
                    + " but length was {}", tableName, MAXIMUM_LENGTH, tableName.length());
      throw new IllegalArgumentException("Table name exceeds 65 character limit: " + tableName);
    }
  }

  public String kafkaName() {
    return kafkaName;
  }

  public String tableName() {
    return tableName;
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
        && Objects.equals(this.tableName, tableName.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kafkaName, tableName);
  }

  @Override
  public String toString() {
    return kafkaName + "(" + tableName + ")";
  }
}
