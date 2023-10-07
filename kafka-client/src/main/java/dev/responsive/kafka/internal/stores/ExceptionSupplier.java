package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.config.ResponsiveStreamsConfig;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.streams.StreamsConfig;

public class ExceptionSupplier {

  private final boolean eosEnabled;

  ExceptionSupplier(final boolean eosEnabled) {
    this.eosEnabled = eosEnabled;
  }

  public static ExceptionSupplier fromConfig(final ResponsiveConfig config) {
    final String processingGuarantee = ResponsiveStreamsConfig.streamsConfig(config.originals())
        .getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG);

    // only eos-v2 is allowed in Responsive, so we can ignore eos-v1 here
    return new ExceptionSupplier(processingGuarantee.equals(StreamsConfig.EXACTLY_ONCE_V2));
  }

  public RuntimeException commitFencedException(final String message) {
    if (eosEnabled) {
      throw new ProducerFencedException(message);
    } else {
      throw new CommitFailedException(message);
    }
  }
}
