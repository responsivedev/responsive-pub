package dev.responsive.k8s.crd.kafkastreams;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Optional;

public class ExpectedLatencyDiagnoserSpec {
  private final int maxExpectedLatencySeconds;
  private final Optional<Integer> windowSeconds;
  private final Optional<Integer> projectionSeconds;
  private final Optional<Integer> scaleDownBufferSeconds;
  private final Optional<Integer> graceSeconds;
  private final Optional<Integer> staggerSeconds;
  private final ScaleUpStrategySpec scaleUpStrategy;

  @JsonCreator
  public ExpectedLatencyDiagnoserSpec(
      @JsonProperty("maxExpectedLatencySeconds") final int maxExpectedLatencySeconds,
      @JsonProperty("windowSeconds") final Optional<Integer> windowSeconds,
      @JsonProperty("projectionSeconds") final Optional<Integer> projectionSeconds,
      @JsonProperty("scaledownBufferSeconds") final Optional<Integer> scaleDownBufferSeconds,
      @JsonProperty("graceSeconds") final Optional<Integer> graceSeconds,
      @JsonProperty("staggerSeconds") final Optional<Integer> staggerSeconds,
      @JsonProperty("scaleUpStrategy") final ScaleUpStrategySpec scaleUpStrategy
  ) {
    this.maxExpectedLatencySeconds = maxExpectedLatencySeconds;
    this.windowSeconds = windowSeconds;
    this.projectionSeconds = projectionSeconds;
    this.scaleDownBufferSeconds = scaleDownBufferSeconds;
    this.graceSeconds = graceSeconds;
    this.staggerSeconds = staggerSeconds;
    this.scaleUpStrategy = scaleUpStrategy;
  }

  public int getMaxExpectedLatencySeconds() {
    return maxExpectedLatencySeconds;
  }

  public Optional<Integer> getWindowSeconds() {
    return windowSeconds;
  }

  public Optional<Integer> getProjectionSeconds() {
    return projectionSeconds;
  }

  public Optional<Integer> getScaleDownBufferSeconds() {
    return scaleDownBufferSeconds;
  }

  public Optional<Integer> getGraceSeconds() {
    return graceSeconds;
  }

  public Optional<Integer> getStaggerSeconds() {
    return staggerSeconds;
  }

  public ScaleUpStrategySpec getScaleUpStrategy() {
    return scaleUpStrategy;
  }
}
