package dev.responsive.k8s.crd.kafkastreams;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ExpectedLatencyDiagnoserSpec {
  private final int maxExpectedLatencySeconds;
  private final int windowSeconds;
  private final int projectionSeconds;
  private final int scaleDownBufferSeconds;
  private final int graceSeconds;
  private final int staggerSeconds;
  private final ScaleUpStrategySpec scaleUpStrategy;

  @JsonCreator
  public ExpectedLatencyDiagnoserSpec(
      @JsonProperty("maxExpectedLatencySeconds") final int maxExpectedLatencySeconds,
      @JsonProperty("windowSeconds") final int windowSeconds,
      @JsonProperty("projectionSeconds") final int projectionSeconds,
      @JsonProperty("scaledownBufferSeconds") final int scaleDownBufferSeconds,
      @JsonProperty("graceSeconds") final int graceSeconds,
      @JsonProperty("staggerSeconds") final int staggerSeconds,
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

  public int getWindowSeconds() {
    return windowSeconds;
  }

  public int getProjectionSeconds() {
    return projectionSeconds;
  }

  public int getScaleDownBufferSeconds() {
    return scaleDownBufferSeconds;
  }

  public int getGraceSeconds() {
    return graceSeconds;
  }

  public int getStaggerSeconds() {
    return staggerSeconds;
  }

  public ScaleUpStrategySpec getScaleUpStrategy() {
    return scaleUpStrategy;
  }
}
