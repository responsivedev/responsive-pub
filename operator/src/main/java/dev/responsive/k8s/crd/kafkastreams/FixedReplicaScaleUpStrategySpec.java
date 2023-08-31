package dev.responsive.k8s.crd.kafkastreams;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FixedReplicaScaleUpStrategySpec {
  private final int replicas;

  @JsonCreator
  public FixedReplicaScaleUpStrategySpec(@JsonProperty("replicas") final int replicas) {
    this.replicas = replicas;
  }

  public int getReplicas() {
    return replicas;
  }
}
