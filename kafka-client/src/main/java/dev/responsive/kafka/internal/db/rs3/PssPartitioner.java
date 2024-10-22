package dev.responsive.kafka.internal.db.rs3;

import java.util.List;

public interface PssPartitioner {
  int pss(byte[] key, int partition);

  List<Integer> allPss();
}
