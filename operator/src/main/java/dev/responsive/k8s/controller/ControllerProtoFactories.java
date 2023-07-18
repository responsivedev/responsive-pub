/*
 * Copyright 2023 Responsive Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.responsive.k8s.controller;

import dev.responsive.k8s.crd.DemoPolicy;
import dev.responsive.k8s.crd.ResponsivePolicy;
import dev.responsive.k8s.crd.ResponsivePolicySpec;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import responsive.controller.v1.controller.proto.ControllerOuterClass;
import responsive.controller.v1.controller.proto.ControllerOuterClass.ApplicationPolicy;
import responsive.controller.v1.controller.proto.ControllerOuterClass.ApplicationState;
import responsive.controller.v1.controller.proto.ControllerOuterClass.DemoPolicy.Diagnoser;
import responsive.controller.v1.controller.proto.ControllerOuterClass.DemoPolicy.DiagnoserType;
import responsive.controller.v1.controller.proto.ControllerOuterClass.DemoPolicy.ProcessingRateDiagnoser;

public final class ControllerProtoFactories {
  public static ControllerOuterClass.UpsertPolicyRequest upsertPolicyRequest(
      final ResponsivePolicy policy) {
    return ControllerOuterClass.UpsertPolicyRequest.newBuilder()
        .setPolicy(ControllerProtoFactories.policyFromK8sResource(policy.getSpec()))
        // TODO(rohan): dont just use a namespaced (w/ /) name
        .setApplicationId(getFullApplicationName(policy))
        .build();
  }

  public static ControllerOuterClass.CurrentStateRequest currentStateRequest(
      final ResponsivePolicy policy,
      final ApplicationState state
  ) {
    return ControllerOuterClass.CurrentStateRequest.newBuilder()
        .setApplicationId(getFullApplicationName(policy))
        .setState(state)
        .build();
  }

  public static ControllerOuterClass.EmptyRequest emptyRequest(final ResponsivePolicy policy) {
    return ControllerOuterClass.EmptyRequest.newBuilder()
        .setApplicationId(getFullApplicationName(policy))
        .build();
  }

  private static ProcessingRateDiagnoser processingRateDiagnoserFromK8sResource(
      final DemoPolicy.ProcessingRateDiagnoser diagnoser
  ) {
    final var builder = ProcessingRateDiagnoser.newBuilder()
        .setRate(diagnoser.getRate());
    if (diagnoser.getWindowMs().isPresent()) {
      builder.setWindowMs(diagnoser.getWindowMs().getAsInt());
    }
    return builder.build();
  }

  private static Diagnoser diagnoserFromK8sResource(final DemoPolicy.Diagnoser diagnoser) {
    switch (diagnoser.getType()) {
      case LAG_SCALE_UP:
        return Diagnoser.newBuilder()
            .setType(DiagnoserType.DIAGNOSER_TYPE_LAG)
            .build();
      case PROCESSING_RATE_SCALE_UP: {
        return Diagnoser.newBuilder()
            .setType(DiagnoserType.DIAGNOSER_TYPE_PROCESSING_RATE_SCALE_UP)
            .setProcessingRateScaleUp(
                processingRateDiagnoserFromK8sResource(
                    diagnoser.getProcessingRateScaleUp().get()))
            .build();
      }
      case PROCESSING_RATE_SCALE_DOWN: {
        return Diagnoser.newBuilder()
            .setType(DiagnoserType.DIAGNOSER_TYPE_PROCESSING_RATE_SCALE_DOWN)
            .setProcessingRateScaleDown(
                processingRateDiagnoserFromK8sResource(
                    diagnoser.getProcessingRateScaleDown().get()))
            .build();
      }
      default:
        throw new IllegalStateException();
    }
  }

  private static List<Diagnoser> diagnosersFromK8sResource(
      final Optional<List<DemoPolicy.Diagnoser>> diagnosers
  ) {
    if (diagnosers.isEmpty()) {
      return Collections.emptyList();
    }
    return diagnosers.get().stream()
        .map(ControllerProtoFactories::diagnoserFromK8sResource)
        .collect(Collectors.toList());
  }

  private static ApplicationPolicy policyFromK8sResource(final ResponsivePolicySpec policySpec) {
    final var builder = ApplicationPolicy.newBuilder();
    switch (policySpec.getPolicyType()) {
      case DEMO:
        assert policySpec.getDemoPolicy().isPresent();
        final var demoPolicy = policySpec.getDemoPolicy().get();
        builder.setDemoPolicy(ControllerOuterClass.DemoPolicy.newBuilder()
            .setMaxReplicas(demoPolicy.getMaxReplicas())
            .setMinReplicas(demoPolicy.getMinReplicas())
            .addAllDiagnoser(diagnosersFromK8sResource(demoPolicy.getDiagnosers()))
            .build()
        );
        break;
      default:
        throw new IllegalStateException("Unexpected type: " + policySpec.getPolicyType());
    }
    builder.setStatus(policySpec.getStatus());
    return builder.build();
  }

  private static String getFullApplicationName(final ResponsivePolicy policy) {
    return policy.getSpec().getApplicationNamespace() + "/" + policy.getSpec().getApplicationName();
  }
}
