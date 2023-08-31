package dev.responsive.k8s.crd.kafkastreams;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class KafkaStreamsPolicySpec {

  private final int maxReplicas;
  private final int minReplicas;
  private final int maxScaleUpReplicas;
  private final Optional<List<DiagnoserSpec>> diagnosers;

  @JsonCreator
  public KafkaStreamsPolicySpec(
      @JsonProperty("maxReplicas") final int maxReplicas,
      @JsonProperty("minReplicas") final int minReplicas,
      @JsonProperty("maxScaleUpReplicas") final int maxScaleUpReplicas,
      @JsonProperty("diagnosers") final Optional<List<DiagnoserSpec>> diagnosers
  ) {
    this.maxReplicas = maxReplicas;
    this.minReplicas = minReplicas;
    this.maxScaleUpReplicas = maxScaleUpReplicas;
    this.diagnosers = Objects.requireNonNull(diagnosers);
  }

  public void validate() {
    diagnosers.ifPresent(ds -> ds.forEach(DiagnoserSpec::validate));
  }

  public int getMaxReplicas() {
    return maxReplicas;
  }

  public int getMinReplicas() {
    return minReplicas;
  }

  public int getMaxScaleUpReplicas() {
    return maxScaleUpReplicas;
  }

  public Optional<List<DiagnoserSpec>> getDiagnosers() {
    return diagnosers;
  }
}
