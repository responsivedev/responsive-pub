/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.k8s.crd.kafkastreams;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.responsive.k8s.crd.PolicyCooldownSpec;
import io.fabric8.generator.annotation.ValidationRule;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@ValidationRule(value = "self.maxReplicas >= self.minReplicas")
public class KafkaStreamsPolicySpec {

  private final int maxReplicas;
  private final int minReplicas;
  private final int maxScaleUpReplicas;
  private final Optional<List<DiagnoserSpec>> diagnosers;
  private final Optional<PolicyCooldownSpec> cooldown;

  @JsonCreator
  public KafkaStreamsPolicySpec(
      @JsonProperty("maxReplicas") final int maxReplicas,
      @JsonProperty("minReplicas") final int minReplicas,
      @JsonProperty("maxScaleUpReplicas") final int maxScaleUpReplicas,
      @JsonProperty("diagnosers") final Optional<List<DiagnoserSpec>> diagnosers,
      @JsonProperty("cooldown") final Optional<PolicyCooldownSpec> cooldown
  ) {
    this.maxReplicas = maxReplicas;
    this.minReplicas = minReplicas;
    this.maxScaleUpReplicas = maxScaleUpReplicas;
    this.diagnosers = Objects.requireNonNull(diagnosers);
    this.cooldown = cooldown;
  }

  public void validate() {
    diagnosers.ifPresent(ds -> ds.forEach(DiagnoserSpec::validate));
    cooldown.ifPresent(PolicyCooldownSpec::validate);
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

  public Optional<PolicyCooldownSpec> getCooldown() {
    return cooldown;
  }
}
