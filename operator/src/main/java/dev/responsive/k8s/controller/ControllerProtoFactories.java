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

import dev.responsive.k8s.crd.ResponsivePolicy;
import dev.responsive.k8s.crd.ResponsivePolicySpec;
import responsive.controller.v1.controller.proto.ControllerOuterClass;
import responsive.controller.v1.controller.proto.ControllerOuterClass.ApplicationPolicy;
import responsive.controller.v1.controller.proto.ControllerOuterClass.ApplicationState;

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

  private static ApplicationPolicy policyFromK8sResource(final ResponsivePolicySpec policySpec) {
    final var builder = ApplicationPolicy.newBuilder();
    switch (policySpec.getPolicyType()) {
      case DEMO:
        assert policySpec.getDemoPolicy().isPresent();
        builder.setDemoPolicy(DemoPolicyProtoFactories.demoPolicyFromK8sResource(
            policySpec.getDemoPolicy().get()));
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
