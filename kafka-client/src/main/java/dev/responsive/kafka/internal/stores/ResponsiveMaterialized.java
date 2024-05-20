package dev.responsive.kafka.internal.stores;

import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;

public class ResponsiveMaterialized<K, V, S extends StateStore> extends Materialized<K, V, S> {

  public ResponsiveMaterialized(
      final Materialized<K, V, S> materialized
  ) {
    super(materialized);
  }

  @Override
  public Materialized<K, V, S> withLoggingDisabled() {
    throw new UnsupportedOperationException(
        "Responsive stores are currently incompatible with disabling the changelog. "
            + "Please reach out to us to request this feature.");
  }
}
