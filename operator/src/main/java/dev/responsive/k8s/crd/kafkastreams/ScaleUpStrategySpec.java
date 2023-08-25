package dev.responsive.k8s.crd.kafkastreams;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import java.util.Optional;

public class ScaleUpStrategySpec {
  public enum Type {
    FIXED_REPLICA,
    RATE_BASED,
    SCALE_TO_MAX
  }

  private Type type;
  private Optional<FixedReplicaScaleUpStrategySpec> fixedReplica;

  @JsonCreator
  public ScaleUpStrategySpec(
      final @JsonProperty("type") Type type,
      final @JsonProperty("fixedReplica") Optional<FixedReplicaScaleUpStrategySpec> fixedReplica
  ) {
    this.type = Objects.requireNonNull(type);
    this.fixedReplica = Objects.requireNonNull(fixedReplica);
  }

  public static ScaleUpStrategySpec fixedReplica(
      final FixedReplicaScaleUpStrategySpec fixedReplicaScaleUpStrategySpec) {
    return new ScaleUpStrategySpec(
        Type.FIXED_REPLICA, Optional.of(fixedReplicaScaleUpStrategySpec));
  }

  public static ScaleUpStrategySpec rateBased() {
    return new ScaleUpStrategySpec(Type.RATE_BASED, Optional.empty());
  }

  public static ScaleUpStrategySpec scaleToMax() {
    return new ScaleUpStrategySpec(Type.SCALE_TO_MAX, Optional.empty());
  }

  public Type getType() {
    return type;
  }

  public Optional<FixedReplicaScaleUpStrategySpec> getFixedReplica() {
    return fixedReplica;
  }
}
