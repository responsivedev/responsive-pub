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

package dev.responsive.kafka.internal.db.rs3.client;

public abstract class WalEntry {
  public abstract void visit(Visitor visitor);

  public interface Visitor {
    void visit(Put put);

    void visit(Delete delete);

    void visit(WindowedDelete windowedDelete);

    void visit(WindowedPut windowedPut);
  }
}
