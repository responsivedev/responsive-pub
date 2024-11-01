package dev.responsive.kafka.internal.db.rs3;

import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import dev.responsive.kafka.internal.db.rs3.client.LssId;
import java.util.Objects;
import org.apache.kafka.common.utils.Bytes;

public class PssTablePartitioner implements TablePartitioner<Bytes, Integer> {
  private final PssPartitioner pssPartitioner;

  public PssTablePartitioner(final PssPartitioner pssPartitioner) {
    this.pssPartitioner = Objects.requireNonNull(pssPartitioner);
  }

  @Override
  public Integer tablePartition(int kafkaPartition, Bytes key) {
    return pssPartitioner.pss(key.get(), new LssId(kafkaPartition));
  }

  @Override
  public Integer metadataTablePartition(int kafkaPartition) {
    throw new UnsupportedOperationException("no metadata table for pss");
  }
}
