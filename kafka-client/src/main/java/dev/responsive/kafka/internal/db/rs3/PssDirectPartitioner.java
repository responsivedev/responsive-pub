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

package dev.responsive.kafka.internal.db.rs3;

import dev.responsive.kafka.internal.db.rs3.client.LssId;
import java.util.List;

/**
 * PSS partitioner that does a 1:1 mapping from LSS to PSS
 */
public class PssDirectPartitioner implements PssPartitioner {
  @Override
  public int pss(byte[] key, LssId lssId) {
    return lssId.id();
  }

  @Override
  public List<Integer> pssForLss(LssId lssId) {
    return List.of(lssId.id());
  }
}
