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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import dev.responsive.k8s.controller.ControllerProtoFactories;
import dev.responsive.k8s.crd.ResponsivePolicy;
import dev.responsive.k8s.crd.ResponsivePolicySpec;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import responsive.controller.v1.controller.proto.ControllerOuterClass;
import responsive.controller.v1.controller.proto.ControllerOuterClass.ApplicationState;
import responsive.controller.v1.controller.proto.ControllerOuterClass.DemoPolicy;
import responsive.controller.v1.controller.proto.ControllerOuterClass.PolicyStatus;

class ControllerProtoFactoriesTest {
  private final ResponsivePolicy demoPolicy = new ResponsivePolicy();
  private ApplicationState demoApplicationState;

  @BeforeEach
  public void setup() {
    final var spec = new ResponsivePolicySpec(
        "gouda",
        "cheddar",
        PolicyStatus.POLICY_STATUS_MANAGED,
        ResponsivePolicySpec.PolicyType.DEMO,
        Optional.of(new ResponsivePolicySpec.DemoPolicy(123))
    );
    demoPolicy.setSpec(spec);
    final var demoMetadata = new ObjectMeta();
    demoMetadata.setNamespace("orange");
    demoMetadata.setName("banana");
    demoPolicy.setMetadata(demoMetadata);
    demoApplicationState = ApplicationState.newBuilder()
        .setDemoState(ControllerOuterClass.DemoApplicationState.newBuilder().setReplicas(3).build())
        .build();
  }

  @Test
  public void shouldCreateUpsertPolicyRequestForDemoPolicy() {
    // when:
    final var request = ControllerProtoFactories.upsertPolicyRequest(demoPolicy);

    // then:
    assertThat(request.getApplicationId(), is("gouda/cheddar"));
    assertThat(request.getPolicy().hasDemoPolicy(), is(true));
    assertThat(request.getPolicy().getStatus(), is(PolicyStatus.POLICY_STATUS_MANAGED));
    final DemoPolicy demoPolicy = request.getPolicy().getDemoPolicy();
    assertThat(demoPolicy.getMaxReplicas(), is(123));
  }

  @Test
  public void shouldCreateCurrentStateRequestForDeployment() {
    // when:
    final var request
        = ControllerProtoFactories.currentStateRequest(demoPolicy, demoApplicationState);

    // Then:
    assertThat(request.getApplicationId(), is("gouda/cheddar"));
    assertThat(request.getState().hasDemoState(), is(true));
    assertThat(request.getState().getDemoState().getReplicas(), is(3));
  }

  @Test
  public void shouldCreateEmptyRequest() {
    final var request = ControllerProtoFactories.emptyRequest(demoPolicy);

    assertThat(request.getApplicationId(), is("gouda/cheddar"));
  }
}