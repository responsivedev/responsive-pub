package dev.responsive.k8s.crd.kafkastreams;

public class FixedReplicaScaleUpStrategySpec {
  private final int replicas;

  public FixedReplicaScaleUpStrategySpec(final int replicas) {
    this.replicas = replicas;
  }

  public int getReplicas() {
    return replicas;
  }
}
