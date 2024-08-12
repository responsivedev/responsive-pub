package dev.responsive.kafka.internal.db.pocket;

import java.util.List;

public interface PssPartitioner {
  int pss(byte[] key, int partition);

  List<Integer> allPss();
}
