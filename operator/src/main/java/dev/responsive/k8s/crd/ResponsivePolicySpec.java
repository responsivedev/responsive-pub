/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.k8s.crd;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.responsive.k8s.crd.kafkastreams.DemoPolicySpec;
import dev.responsive.k8s.crd.kafkastreams.KafkaStreamsPolicySpec;
import java.util.Objects;
import java.util.Optional;
import responsive.controller.v1.controller.proto.ControllerOuterClass.PolicyStatus;

public class ResponsivePolicySpec {
  private final String applicationNamespace;
  private final String applicationName;
  private final String applicationId;
  // TODO: dont use the protobuf enum type in the k8s crd definition
  private final PolicyStatus status;
  private final ResponsivePolicySpec.PolicyType policyType;
  private final Optional<DemoPolicySpec> demoPolicy;
  private final Optional<KafkaStreamsPolicySpec> kafkaStreamsPolicy;

  public enum PolicyType {
    DEMO,
    KAFKA_STREAMS
  }

  @JsonCreator
  public ResponsivePolicySpec(
      @JsonProperty("applicationNamespace") final String applicationNamespace,
      @JsonProperty("applicationName") final String applicationName,
      @JsonProperty("applicationId") final String applicationId,
      @JsonProperty("status") final PolicyStatus status,
      @JsonProperty("policyType") final PolicyType policyType,
      @JsonProperty("demoPolicy") final Optional<DemoPolicySpec> demoPolicy,
      @JsonProperty("kafkaStreamsPolicy") final Optional<KafkaStreamsPolicySpec> kafkaStreamsPolicy
  ) {
    this.applicationNamespace = applicationNamespace;
    this.applicationName = applicationName;
    // for backwards compatibility we allow this to be null
    this.applicationId = applicationId == null
        ? applicationNamespace + "/" + applicationName
        : applicationId;
    this.status = status;
    this.demoPolicy = Optional.empty();
    if (Objects.equals(policyType, PolicyType.DEMO)) {
      this.policyType = PolicyType.KAFKA_STREAMS;
      this.kafkaStreamsPolicy
          = Optional.of(toKafkaStreamsPolicy(Objects.requireNonNull(demoPolicy).get()));
    } else {
      this.policyType = policyType;
      this.kafkaStreamsPolicy = Objects.requireNonNull(kafkaStreamsPolicy);
    }
  }

  public void validate() {
    Objects.requireNonNull(applicationName, "applicationName");
    Objects.requireNonNull(applicationNamespace, "applicationNamespace");
    Objects.requireNonNull(applicationId, "applicationId");
    Objects.requireNonNull(status, "status");
    Objects.requireNonNull(policyType, "policyType");
    switch (policyType) {
      case KAFKA_STREAMS:
        CrdUtils.validatePresent(kafkaStreamsPolicy, "kafkaStreamsPolicy").validate();
        break;
      default:
        break;
    }
  }

  public String getApplicationNamespace() {
    return applicationNamespace;
  }

  public String getApplicationName() {
    return applicationName;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public PolicyStatus getStatus() {
    return status;
  }

  public PolicyType getPolicyType() {
    return policyType;
  }

  public Optional<KafkaStreamsPolicySpec> getKafkaStreamsPolicy() {
    return kafkaStreamsPolicy;
  }

  public Optional<DemoPolicySpec> getDemoPolicy() {
    return demoPolicy;
  }

  private static KafkaStreamsPolicySpec toKafkaStreamsPolicy(final DemoPolicySpec demoPolicy) {
    return new KafkaStreamsPolicySpec(
        demoPolicy.getMaxReplicas(),
        demoPolicy.getMinReplicas(),
        demoPolicy.getMaxScaleUpReplicas(),
        demoPolicy.getDiagnosers(),
        demoPolicy.getCooldown()
    );
  }
}
