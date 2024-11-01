package dev.responsive.kafka.internal.db.rs3;

import dev.responsive.kafka.internal.db.rs3.client.LssId;
import java.util.List;

public interface PssPartitioner {
  int pss(byte[] key, LssId lssId);

  List<Integer> pssForLss(LssId lssId);
}
