package dev.responsive.k8s.crd.kafkastreams;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MeanSojournTimeDiagnoserSpec {
  private final int maxMeanSojournTime;
  private final ScaleUpStrategySpec scaleUpStrategy;

  @JsonCreator
  public MeanSojournTimeDiagnoserSpec(
      @JsonProperty("maxMeanSojournTime") final int maxMeanSojournTime,
      @JsonProperty("scaleUpStrategy") final ScaleUpStrategySpec scaleUpStrategy
  ) {
    this.maxMeanSojournTime = maxMeanSojournTime;
    this.scaleUpStrategy = scaleUpStrategy;
  }

  public int getMaxMeanSojournTime() {
    return maxMeanSojournTime;
  }

  public ScaleUpStrategySpec getScaleUpStrategy() {
    return scaleUpStrategy;
  }
}
