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

package dev.responsive.controller;

import dev.responsive.k8s.crd.ResponsivePolicy;
import dev.responsive.k8s.crd.ResponsivePolicySpec;
import responsive.controller.v1.controller.proto.ControllerOuterClass;
import responsive.controller.v1.controller.proto.ControllerOuterClass.ApplicationPolicy;
import responsive.controller.v1.controller.proto.ControllerOuterClass.ApplicationState;
import responsive.controller.v1.controller.proto.ControllerOuterClass.DemoPolicy;

public final class ControllerProtoFactories {

  public static ControllerOuterClass.UpsertPolicyRequest upsertPolicyRequest(
      final ResponsivePolicy policy
  ) {
    return ControllerOuterClass.UpsertPolicyRequest.newBuilder()
        .setPolicy(ControllerProtoFactories.policyFromK8sResource(policy.getSpec()))
        // TODO(rohan): dont just use a namespaced (w/ /) name
        .setApplicationId(getNamespacedName(policy))
        .build();
  }

  public static ControllerOuterClass.CurrentStateRequest currentStateRequest(
      final ResponsivePolicy policy,
      final ApplicationState state
  ) {
    return ControllerOuterClass.CurrentStateRequest.newBuilder()
        .setApplicationId(getNamespacedName(policy))
        .setState(state)
        .build();
  }

  public static ControllerOuterClass.EmptyRequest emptyRequest(final ResponsivePolicy policy) {
    return ControllerOuterClass.EmptyRequest.newBuilder()
        .setApplicationId(getNamespacedName(policy))
        .build();
  }

  private static ApplicationPolicy policyFromK8sResource(final ResponsivePolicySpec policySpec) {
    final var builder = ApplicationPolicy.newBuilder();
    switch (policySpec.policyType()) {
      case DEMO -> {
        assert policySpec.demoPolicy().isPresent();
        final var demoPolicy = policySpec.demoPolicy().get();
        builder.setDemoPolicy(DemoPolicy.newBuilder()
            .setMaxReplicas(demoPolicy.maxReplicas())
            .build()
        );
      }
      default -> throw new IllegalStateException("Unexpected value: " + policySpec.policyType());
    }
    return builder.build();
  }

  private static String getNamespacedName(final ResponsivePolicy policy) {
    return policy.getMetadata().getNamespace() + "/" + policy.getMetadata().getName();
  }

}
