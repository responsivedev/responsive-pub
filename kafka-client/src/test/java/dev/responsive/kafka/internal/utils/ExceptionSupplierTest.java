package dev.responsive.kafka.internal.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

public class ExceptionSupplierTest {

  private static final String ERROR_MSG = "Commit failed, you were fenced yo!";

  private static final Map<String, String> BASE_PROPS = Map.of(
      StreamsConfig.APPLICATION_ID_CONFIG, "my-test-app",
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:666"
  );

  @Test
  public void shouldLoadFromConfigWithAlos() {
    final Map<String, String> props = new HashMap<>(BASE_PROPS);
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
    final ResponsiveConfig config = ResponsiveConfig.loggedConfig(props);

    final ExceptionSupplier exceptionSupplier = ExceptionSupplier.fromConfig(config);
    assertFalse(exceptionSupplier.eosEnabled());
  }

  @Test
  public void shouldLoadFromConfigWithEos() {
    final Map<String, String> props = new HashMap<>(BASE_PROPS);
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    final ResponsiveConfig config = ResponsiveConfig.loggedConfig(props);

    final ExceptionSupplier exceptionSupplier = ExceptionSupplier.fromConfig(config);
    assertTrue(exceptionSupplier.eosEnabled());
  }

  @Test
  public void shouldLoadFromConfigWithProcessingGuaranteeUndefined() {
    final Map<String, String> props = new HashMap<>(BASE_PROPS);
    final ResponsiveConfig config = ResponsiveConfig.loggedConfig(props);

    final ExceptionSupplier exceptionSupplier = ExceptionSupplier.fromConfig(config);
    assertFalse(exceptionSupplier.eosEnabled());
  }

  @Test
  public void shouldReturnCommitFailedExceptionForCommitFencedWithAlos() {
    final ExceptionSupplier exceptionSupplier = new ExceptionSupplier(false);
    final RuntimeException exception = exceptionSupplier.commitFencedException(ERROR_MSG);

    assertTrue(exception instanceof CommitFailedException);
    assertThat(exception.getMessage(), equalTo(ERROR_MSG));
  }

  @Test
  public void shouldReturnProducerFencedExceptionForCommitFencedWithEos() {
    final ExceptionSupplier exceptionSupplier = new ExceptionSupplier(true);
    final RuntimeException exception = exceptionSupplier.commitFencedException(ERROR_MSG);

    assertTrue(exception instanceof ProducerFencedException);
    assertThat(exception.getMessage(), equalTo(ERROR_MSG));
  }

}
