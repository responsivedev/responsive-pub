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
