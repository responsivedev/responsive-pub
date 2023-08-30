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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

import dev.responsive.k8s.crd.ResponsivePolicy;
import dev.responsive.k8s.crd.ResponsivePolicySpec;
import dev.responsive.k8s.crd.kafkastreams.DemoPolicySpec;
import dev.responsive.k8s.crd.kafkastreams.DiagnoserSpec;
import dev.responsive.k8s.crd.kafkastreams.FixedReplicaScaleUpStrategySpec;
import dev.responsive.k8s.crd.kafkastreams.MeanSojournTimeDiagnoserSpec;
import dev.responsive.k8s.crd.kafkastreams.RateBasedDiagnoserSpec;
import dev.responsive.k8s.crd.kafkastreams.ScaleUpStrategySpec;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import responsive.controller.v1.controller.proto.ControllerOuterClass;

class ControllerProtoFactoriesTest {
  private static final String TESTENV = "testenv";

  private final ResponsivePolicy demoPolicy = new ResponsivePolicy();
  private ControllerOuterClass.ApplicationState demoApplicationState;

  @BeforeEach
  public void setup() {
    final var spec = new ResponsivePolicySpec(
        "gouda",
        "cheddar",
        ControllerOuterClass.PolicyStatus.POLICY_STATUS_MANAGED,
        ResponsivePolicySpec.PolicyType.DEMO,
        Optional.of(new DemoPolicySpec(
            123, 7, 1, Optional.empty()))
    );
    demoPolicy.setSpec(spec);
    final var demoMetadata = new ObjectMeta();
    demoMetadata.setNamespace("orange");
    demoMetadata.setName("banana");
    demoPolicy.setMetadata(demoMetadata);
    demoApplicationState = ControllerOuterClass.ApplicationState.newBuilder()
        .setDemoState(ControllerOuterClass.DemoApplicationState.newBuilder().setReplicas(3).build())
        .build();
  }

  @Test
  public void shouldCreateUpsertPolicyRequestForDemoPolicy() {
    // when:
    final var request = ControllerProtoFactories.upsertPolicyRequest(TESTENV, demoPolicy);

    // then:
    assertThat(request.getApplicationId(), is("testenv/gouda/cheddar"));
    assertThat(request.getPolicy().hasDemoPolicy(), is(true));
    assertThat(
        request.getPolicy().getStatus(),
        is(ControllerOuterClass.PolicyStatus.POLICY_STATUS_MANAGED)
    );
    final ControllerOuterClass.DemoPolicySpec demoPolicy = request.getPolicy().getDemoPolicy();
    assertThat(demoPolicy.getMaxReplicas(), is(123));
    assertThat(demoPolicy.getMinReplicas(), is(7));
  }

  private ResponsivePolicySpec specWithDiagnoser(
      final DiagnoserSpec diagnoserSpec) {
    return new ResponsivePolicySpec(
        "gouda",
        "cheddar",
        ControllerOuterClass.PolicyStatus.POLICY_STATUS_MANAGED,
        ResponsivePolicySpec.PolicyType.DEMO,
        Optional.of(new DemoPolicySpec(
            123, 7, 1, Optional.of(List.of(diagnoserSpec))))
    );
  }

  @Test
  public void shouldCreateUpsertPolicyRequestForDemoPolicyWithLagDiagnoser() {
    // given:
    demoPolicy.setSpec(specWithDiagnoser(DiagnoserSpec.lag()));

    // when:
    final var request = ControllerProtoFactories.upsertPolicyRequest(TESTENV, demoPolicy);

    // then:
    final ControllerOuterClass.DemoPolicySpec created = request.getPolicy().getDemoPolicy();
    assertThat(created.getDiagnoserList(), contains(
        ControllerOuterClass.DemoPolicySpec.DiagnoserSpec.newBuilder()
            .setLagScaleUp(
                ControllerOuterClass.DemoPolicySpec.LagDiagnoserSpec.newBuilder().build())
            .build()
    ));
  }

  @Test
  public void shouldCreateUpsertPolicyRequestForDemoPolicyWithRateScaleUpDiagnoser() {
    // given:
    demoPolicy.setSpec(
        specWithDiagnoser(
            DiagnoserSpec.processingRateScaleUp(
                new RateBasedDiagnoserSpec(10, Optional.of(123))))
    );

    // when:
    final var request = ControllerProtoFactories.upsertPolicyRequest(TESTENV, demoPolicy);

    // then:
    final ControllerOuterClass.DemoPolicySpec created = request.getPolicy().getDemoPolicy();
    assertThat(created.getDiagnoserList(), contains(
        ControllerOuterClass.DemoPolicySpec.DiagnoserSpec.newBuilder()
            .setProcessingRateScaleUp(ControllerOuterClass.DemoPolicySpec.RateBasedDiagnoserSpec
                .newBuilder()
                .setRate(10)
                .setWindowMs(123)
                .build())
            .build()
    ));
  }

  @Test
  public void shouldCreateUpsertPolicyRequestForDemoPolicyWithRateScaleDownDiagnoser() {
    // given:
    demoPolicy.setSpec(
        specWithDiagnoser(
            DiagnoserSpec.processingRateScaleDown(
                new RateBasedDiagnoserSpec(10, Optional.of(123))))
    );

    // when:
    final var request = ControllerProtoFactories.upsertPolicyRequest(TESTENV, demoPolicy);

    // then:
    final ControllerOuterClass.DemoPolicySpec created = request.getPolicy().getDemoPolicy();
    assertThat(created.getDiagnoserList(), contains(
        ControllerOuterClass.DemoPolicySpec.DiagnoserSpec.newBuilder()
            .setProcessingRateScaleDown(ControllerOuterClass.DemoPolicySpec.RateBasedDiagnoserSpec
                .newBuilder()
                .setRate(10)
                .setWindowMs(123)
                .build())
            .build()
    ));
  }

  @Test
  public void shouldCreateUpsertPolicyRequestWithApplicationIdWhenEnvEmpty() {
    // when:
    final var request = ControllerProtoFactories.upsertPolicyRequest("", demoPolicy);

    // then:
    assertThat(request.getApplicationId(), is("gouda/cheddar"));
  }

  @Test
  public void shouldCreateUpsertPolicyRequestWithMeanSojournTimeDiagnoserWithFixedReplicaScaling() {
    // given:
    demoPolicy.setSpec(
        specWithDiagnoser(DiagnoserSpec.meanSojournTime(
            new MeanSojournTimeDiagnoserSpec(
                123,
                ScaleUpStrategySpec.fixedReplica(new FixedReplicaScaleUpStrategySpec(3))
            )
        ))
    );

    // when:
    final var request = ControllerProtoFactories.upsertPolicyRequest("", demoPolicy);

    // then:
    final var diagnoser = request.getPolicy().getDemoPolicy().getDiagnoserList().get(0);
    assertThat(diagnoser.hasMeanSojournTime(), is(true));
    assertThat(diagnoser.getMeanSojournTime().getMaxMeanSojournTimeSeconds(), is(123));
    assertThat(diagnoser.getMeanSojournTime().hasFixedReplicas(), is(true));
    assertThat(diagnoser.getMeanSojournTime().getFixedReplicas().getReplicas(), is(3));
  }

  @Test
  public void shouldCreateUpsertPolicyRequestWithMeanSojournTimeDiagnoserWithRateBasedStrategy() {
    // given:
    demoPolicy.setSpec(
        specWithDiagnoser(DiagnoserSpec.meanSojournTime(
            new MeanSojournTimeDiagnoserSpec(
                123,
                ScaleUpStrategySpec.rateBased()
            )
        ))
    );

    // when:
    final var request = ControllerProtoFactories.upsertPolicyRequest("", demoPolicy);

    // then:
    final var diagnoser = request.getPolicy().getDemoPolicy().getDiagnoserList().get(0);
    assertThat(diagnoser.hasMeanSojournTime(), is(true));
    assertThat(diagnoser.getMeanSojournTime().getMaxMeanSojournTimeSeconds(), is(123));
    assertThat(diagnoser.getMeanSojournTime().hasRateBased(), is(true));
  }

  @Test
  public void shouldCreateUpsertPolicyRequestWithMeanSojournTimeDiagnoserWithScaleToMaxStrategy() {
    // given:
    demoPolicy.setSpec(
        specWithDiagnoser(DiagnoserSpec.meanSojournTime(
            new MeanSojournTimeDiagnoserSpec(
                123,
                ScaleUpStrategySpec.scaleToMax()
            )
        ))
    );

    // when:
    final var request = ControllerProtoFactories.upsertPolicyRequest("", demoPolicy);

    // then:
    final var diagnoser = request.getPolicy().getDemoPolicy().getDiagnoserList().get(0);
    assertThat(diagnoser.hasMeanSojournTime(), is(true));
    assertThat(diagnoser.getMeanSojournTime().getMaxMeanSojournTimeSeconds(), is(123));
    assertThat(diagnoser.getMeanSojournTime().hasScaleToMax(), is(true));
  }

  @Test
  public void shouldCreateCurrentStateRequestForDeployment() {
    // when:
    final var request
        = ControllerProtoFactories.currentStateRequest(TESTENV, demoPolicy, demoApplicationState);

    // Then:
    assertThat(request.getApplicationId(), is("testenv/gouda/cheddar"));
    assertThat(request.getState().hasDemoState(), is(true));
    assertThat(request.getState().getDemoState().getReplicas(), is(3));
  }

  @Test
  public void shouldSetAppIdInCurrentStateRequestWhenEnvEmpty() {
    // when:
    final var request
        = ControllerProtoFactories.currentStateRequest("", demoPolicy, demoApplicationState);

    // Then:
    assertThat(request.getApplicationId(), is("gouda/cheddar"));
  }

  @Test
  public void shouldCreateEmptyRequest() {
    final var request = ControllerProtoFactories.emptyRequest(TESTENV, demoPolicy);

    assertThat(request.getApplicationId(), is("testenv/gouda/cheddar"));
  }

  @Test
  public void shouldCreateEmptyRequestWhenEnvEmpty() {
    final var request = ControllerProtoFactories.emptyRequest("", demoPolicy);

    assertThat(request.getApplicationId(), is("gouda/cheddar"));
  }
}
