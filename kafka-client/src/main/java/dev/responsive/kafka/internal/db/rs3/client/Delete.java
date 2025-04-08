/*
 * Copyright 2025 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.db.rs3.client;

import java.util.Arrays;
import java.util.Objects;

public class Delete extends WalEntry {
  private final byte[] key;

  public Delete(final byte[] key) {
    this.key = Objects.requireNonNull(key);
  }

  public byte[] key() {
    return key;
  }

  @Override
  public void visit(final Visitor visitor) {
    visitor.visit(this);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Delete delete = (Delete) o;
    return Objects.deepEquals(key, delete.key);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(key);
  }
}
