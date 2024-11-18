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
