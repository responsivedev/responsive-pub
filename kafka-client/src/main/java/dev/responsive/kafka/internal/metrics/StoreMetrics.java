package dev.responsive.kafka.internal.metrics;

import dev.responsive.kafka.internal.metrics.ResponsiveMetrics.MetricGroup;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;

public class StoreMetrics implements MetricGroup {

  // Responsive store metrics scoped to the individual state store level
  public static final String STORE_METRIC_GROUP = "store-metrics";

  public static final String RESTORE_LATENCY = "restore-latency";
  public static final String RESTORE_LATENCY_AVG = RESTORE_LATENCY + "-avg";
  public static final String RESTORE_LATENCY_MAX = RESTORE_LATENCY + "-max";
  public static final String RESTORE_LATENCY_DESCRIPTION = "amount of time spent restoring this state store before resuming processing";

  private final Map<String, String> tags = new HashMap<>();

  StoreMetrics(
      final Map<String, String> baseAppTags,
      final String threadId,
      final String storeName,
      final TopicPartition changelog
  ) {
    tags.putAll(baseAppTags);
    tags.put("thread", threadId);
    tags.put("store", storeName);
    tags.put("topic", changelog.topic());
    tags.put("partition", String.valueOf(changelog.partition()));
  }
  
  @Override
  public String groupName() {
    return STORE_METRIC_GROUP;
  }

  @Override
  public Map<String, String> tags() {
    return tags;
  }
  
}
