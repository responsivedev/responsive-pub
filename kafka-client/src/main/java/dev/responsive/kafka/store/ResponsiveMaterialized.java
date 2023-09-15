package dev.responsive.kafka.store;

import static dev.responsive.utils.StoreUtil.validateLogConfigs;

import java.util.Map;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;

public class ResponsiveMaterialized<K, V, S extends StateStore> extends Materialized<K, V, S> {

  private final boolean truncateChangelog;

  public ResponsiveMaterialized(
      final Materialized<K, V, S> materialized,
      final boolean truncateChangelog
  ) {
    super(materialized);
    this.truncateChangelog = truncateChangelog;

    // Truncation uses the admin's delete request, which seems to require the cleanup.policy
    // to contain 'delete'
    if (truncateChangelog) {
      topicConfig.put(
          TopicConfig.CLEANUP_POLICY_CONFIG,
          TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE
      );
    }
  }

  @Override
  public Materialized<K, V, S> withLoggingEnabled(final Map<String, String> config) {
    validateLogConfigs(config, truncateChangelog, storeName);
    return super.withLoggingEnabled(config);
  }

  @Override
  public Materialized<K, V, S> withLoggingDisabled() {
    throw new UnsupportedOperationException(
        "Responsive stores are currently incompatible with disabling the changelog. "
            + "Please reach out to us to request this feature.");
  }
}
