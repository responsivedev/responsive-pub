package dev.responsive.kafka.internal.utils;

import dev.responsive.kafka.internal.config.ResponsiveStreamsConfig;
import java.util.Map;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.streams.StreamsConfig;

public class ExceptionSupplier {

  private final boolean eosEnabled;

  public ExceptionSupplier(final boolean eosEnabled) {
    this.eosEnabled = eosEnabled;
  }

  public static ExceptionSupplier fromConfig(final Map<?, ?> props) {
    final String processingGuarantee = ResponsiveStreamsConfig.streamsConfig(props)
        .getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG);

    // only eos-v2 is allowed in Responsive, so we can ignore eos-v1 here
    return new ExceptionSupplier(processingGuarantee.equals(StreamsConfig.EXACTLY_ONCE_V2));
  }

  public RuntimeException commitFencedException(final String message) {
    if (eosEnabled) {
      return new ProducerFencedException(message);
    } else {
      return new CommitFailedException(message);
    }
  }

  public boolean eosEnabled() {
    return eosEnabled;
  }
}
