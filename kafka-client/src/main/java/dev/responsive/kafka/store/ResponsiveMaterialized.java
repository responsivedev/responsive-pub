package dev.responsive.kafka.store;

import static dev.responsive.utils.StoreUtil.validateLogConfigs;

import java.util.Map;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;

public class ResponsiveMaterialized<K, V, S extends StateStore> extends Materialized<K, V, S> {

  private final boolean truncateChangelog;

  public ResponsiveMaterialized(
      final Materialized<K, V, S> materialized, final boolean truncateChangelog) {
    super(materialized);
    this.truncateChangelog = truncateChangelog;
    if (truncateChangelog) {
      topicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
    }
  }

  @Override
  public Materialized<K, V, S> withLoggingEnabled(final Map<String, String> config) {
    validateLogConfigs(config, truncateChangelog);
    return super.withLoggingEnabled(config);
  }
}
