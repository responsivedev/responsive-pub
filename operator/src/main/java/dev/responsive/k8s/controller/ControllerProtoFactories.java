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

package dev.responsive.k8s.controller;

import dev.responsive.k8s.crd.ResponsivePolicy;
import dev.responsive.k8s.crd.ResponsivePolicySpec;
import responsive.controller.v1.controller.proto.ControllerOuterClass;
import responsive.controller.v1.controller.proto.ControllerOuterClass.ApplicationPolicySpec;
import responsive.controller.v1.controller.proto.ControllerOuterClass.ApplicationState;

public final class ControllerProtoFactories {
  public static ControllerOuterClass.UpsertPolicyRequest upsertPolicyRequest(
      final String environment,
      final ResponsivePolicy policy) {
    return ControllerOuterClass.UpsertPolicyRequest.newBuilder()
        .setPolicy(ControllerProtoFactories.policyFromK8sResource(policy.getSpec()))
        // TODO(rohan): dont just use a namespaced (w/ /) name
        .setApplicationId(getFullApplicationName(environment, policy))
        .build();
  }

  public static ControllerOuterClass.CurrentStateRequest currentStateRequest(
      final String environment,
      final ResponsivePolicy policy,
      final ApplicationState state
  ) {
    return ControllerOuterClass.CurrentStateRequest.newBuilder()
        .setApplicationId(getFullApplicationName(environment, policy))
        .setState(state)
        .build();
  }

  public static ControllerOuterClass.UpdateActionStatusRequest updateActionStatusRequest(
      final String environment,
      final ResponsivePolicy policy,
      final String actionId,
      final ControllerOuterClass.ActionStatus.Status status,
      final String reason
  ) {
    return ControllerOuterClass.UpdateActionStatusRequest.newBuilder()
        .setApplicationId(getFullApplicationName(environment, policy))
        .setActionId(actionId)
        .setStatus(ControllerOuterClass.ActionStatus.newBuilder()
            .setStatus(status)
            .setDetails(reason))
        .build();
  }

  public static ControllerOuterClass.EmptyRequest emptyRequest(
      final String environment,
      final ResponsivePolicy policy
  ) {
    return ControllerOuterClass.EmptyRequest.newBuilder()
        .setApplicationId(getFullApplicationName(environment, policy))
        .build();
  }

  private static ApplicationPolicySpec policyFromK8sResource(
      final ResponsivePolicySpec policySpec) {
    final var builder = ApplicationPolicySpec.newBuilder();
    switch (policySpec.getPolicyType()) {
      case KAFKA_STREAMS:
        assert policySpec.getKafkaStreamsPolicy().isPresent();
        builder.setKafkaStreamsPolicy(KafkaStreamsPolicyProtoFactories
            .kafkaStreamsPolicyFromK8sResource(policySpec.getKafkaStreamsPolicy().get()));
        break;
      default:
        throw new IllegalStateException("Unexpected type: " + policySpec.getPolicyType());
    }
    builder.setStatus(policySpec.getStatus());
    return builder.build();
  }

  private static String getFullApplicationName(
      final String environment,
      final ResponsivePolicy policy
  ) {
    final String prefix = environment.isEmpty() ? "" : environment + "/";
    return prefix + policy.getSpec().getApplicationId();
  }
}
