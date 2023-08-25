package dev.responsive.k8s.crd.kafkastreams;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.Optional;

public class RateBasedDiagnoserSpec {
  private final int rate;
  // don't use OptionalInt here. The CRD schema generator only handles Optional transparently
  private final Optional<Integer> windowMs;

  public int getRate() {
    return rate;
  }

  public Optional<Integer> getWindowMs() {
    return windowMs;
  }

  @JsonCreator
  public RateBasedDiagnoserSpec(
      @JsonProperty("rate") final int rate,
      @JsonProperty("windowMs") final Optional<Integer> windowMs
  ) {
    this.rate = rate;
    this.windowMs = Objects.requireNonNull(windowMs);
  }
}
