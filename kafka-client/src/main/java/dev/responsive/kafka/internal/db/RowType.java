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

package dev.responsive.kafka.internal.db;

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;

public enum RowType {

  UNKNOWN((byte) 0x0),
  METADATA_ROW((byte) 0x1),
  DATA_ROW((byte) 0x2);

  private final byte val;

  RowType(final byte val) {
    this.val = val;
  }

  public byte val() {
    return val;
  }

  public Term literal() {
    return QueryBuilder.literal(val);
  }
}
