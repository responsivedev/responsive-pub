package dev.responsive.k8s.crd.kafkastreams;

public class MeanSojournTimeDiagnoserSpec {
  private final int maxMeanSojournTime;
  private final ScaleUpStrategySpec scaleUpStrategy;

  public MeanSojournTimeDiagnoserSpec(
      final int maxMeanSojournTime,
      final ScaleUpStrategySpec scaleUpStrategy
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
