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

import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import java.util.Objects;

public abstract class PssTablePartitioner<K> implements TablePartitioner<K, Integer> {
  private final PssPartitioner pssPartitioner;

  public PssTablePartitioner(final PssPartitioner pssPartitioner) {
    this.pssPartitioner = Objects.requireNonNull(pssPartitioner);
  }

  public abstract byte[] serialize(K key);

  @Override
  public Integer tablePartition(int kafkaPartition, K key) {
    final var serializedKey = serialize(key);
    return pssPartitioner.pss(serializedKey, new LssId(kafkaPartition));
  }

  @Override
  public Integer metadataTablePartition(int kafkaPartition) {
    throw new UnsupportedOperationException("no metadata table for pss");
  }
}
