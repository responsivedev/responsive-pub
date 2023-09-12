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
import dev.responsive.k8s.crd.kafkastreams.ExpectedLatencyDiagnoserSpec;
import dev.responsive.k8s.crd.kafkastreams.FixedReplicaScaleUpStrategySpec;
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

  private final ResponsivePolicy kafkaStreamsPolicy = new ResponsivePolicy();
  private ControllerOuterClass.ApplicationState kafkaStreamsApplicationState;

  @BeforeEach
  public void setup() {
    final var spec = new ResponsivePolicySpec(
        "gouda",
        "cheddar",
        ControllerOuterClass.PolicyStatus.POLICY_STATUS_MANAGED,
        ResponsivePolicySpec.PolicyType.DEMO,
        Optional.of(new DemoPolicySpec(
            123, 7, 1, Optional.empty())),
        Optional.empty()
    );
    kafkaStreamsPolicy.setSpec(spec);
    final var kafkaStreamsMetadata = new ObjectMeta();
    kafkaStreamsMetadata.setNamespace("orange");
    kafkaStreamsMetadata.setName("banana");
    kafkaStreamsPolicy.setMetadata(kafkaStreamsMetadata);
    kafkaStreamsApplicationState = ControllerOuterClass.ApplicationState.newBuilder()
        .setKafkaStreamsState(
            ControllerOuterClass.KafkaStreamsApplicationState.newBuilder().setReplicas(3).build())
        .build();
  }

  @Test
  public void shouldCreateUpsertPolicyRequestForKafkaStreamsPolicy() {
    // when:
    final var request = ControllerProtoFactories.upsertPolicyRequest(TESTENV, kafkaStreamsPolicy);

    // then:
    assertThat(request.getApplicationId(), is("testenv/gouda/cheddar"));
    assertThat(request.getPolicy().hasKafkaStreamsPolicy(), is(true));
    assertThat(
        request.getPolicy().getStatus(),
        is(ControllerOuterClass.PolicyStatus.POLICY_STATUS_MANAGED)
    );
    final ControllerOuterClass.KafkaStreamsPolicySpec kafkaStreamsPolicy
        = request.getPolicy().getKafkaStreamsPolicy();
    assertThat(kafkaStreamsPolicy.getMaxReplicas(), is(123));
    assertThat(kafkaStreamsPolicy.getMinReplicas(), is(7));
  }

  private ResponsivePolicySpec specWithDiagnoser(
      final DiagnoserSpec diagnoserSpec) {
    return new ResponsivePolicySpec(
        "gouda",
        "cheddar",
        ControllerOuterClass.PolicyStatus.POLICY_STATUS_MANAGED,
        ResponsivePolicySpec.PolicyType.DEMO,
        Optional.of(new DemoPolicySpec(
            123, 7, 1, Optional.of(List.of(diagnoserSpec)))),
        Optional.empty()
    );
  }

  @Test
  public void shouldCreateUpsertPolicyRequestForKafkaStreamsPolicyWithLagDiagnoser() {
    // given:
    kafkaStreamsPolicy.setSpec(specWithDiagnoser(DiagnoserSpec.lag()));

    // when:
    final var request = ControllerProtoFactories.upsertPolicyRequest(TESTENV, kafkaStreamsPolicy);

    // then:
    final ControllerOuterClass.KafkaStreamsPolicySpec created
        = request.getPolicy().getKafkaStreamsPolicy();
    assertThat(created.getDiagnoserList(), contains(
        ControllerOuterClass.KafkaStreamsPolicySpec.DiagnoserSpec.newBuilder()
            .setLagScaleUp(
                ControllerOuterClass.KafkaStreamsPolicySpec.LagDiagnoserSpec.newBuilder().build())
            .build()
    ));
  }

  @Test
  public void shouldCreateUpsertPolicyRequestForKafkaStreamsPolicyWithRateScaleUpDiagnoser() {
    // given:
    kafkaStreamsPolicy.setSpec(
        specWithDiagnoser(
            DiagnoserSpec.processingRateScaleUp(
                new RateBasedDiagnoserSpec(10, Optional.of(123))))
    );

    // when:
    final var request = ControllerProtoFactories.upsertPolicyRequest(TESTENV, kafkaStreamsPolicy);

    // then:
    final ControllerOuterClass.KafkaStreamsPolicySpec created
        = request.getPolicy().getKafkaStreamsPolicy();
    assertThat(created.getDiagnoserList(), contains(
        ControllerOuterClass.KafkaStreamsPolicySpec.DiagnoserSpec.newBuilder()
            .setProcessingRateScaleUp(
                ControllerOuterClass.KafkaStreamsPolicySpec.RateBasedDiagnoserSpec.newBuilder()
                .setRate(10)
                .setWindowMs(123)
                .build())
            .build()
    ));
  }

  @Test
  public void shouldCreateUpsertPolicyRequestForKafkaStreamsPolicyWithRateScaleDownDiagnoser() {
    // given:
    kafkaStreamsPolicy.setSpec(
        specWithDiagnoser(
            DiagnoserSpec.processingRateScaleDown(
                new RateBasedDiagnoserSpec(10, Optional.of(123))))
    );

    // when:
    final var request = ControllerProtoFactories.upsertPolicyRequest(TESTENV, kafkaStreamsPolicy);

    // then:
    final ControllerOuterClass.KafkaStreamsPolicySpec created
        = request.getPolicy().getKafkaStreamsPolicy();
    assertThat(created.getDiagnoserList(), contains(
        ControllerOuterClass.KafkaStreamsPolicySpec.DiagnoserSpec.newBuilder()
            .setProcessingRateScaleDown(
                ControllerOuterClass.KafkaStreamsPolicySpec.RateBasedDiagnoserSpec.newBuilder()
                .setRate(10)
                .setWindowMs(123)
                .build())
            .build()
    ));
  }

  @Test
  public void shouldCreateUpsertPolicyRequestWithApplicationIdWhenEnvEmpty() {
    // when:
    final var request
        = ControllerProtoFactories.upsertPolicyRequest("", kafkaStreamsPolicy);

    // then:
    assertThat(request.getApplicationId(), is("gouda/cheddar"));
  }

  @Test
  public void shouldCreateUpsertPolicyRequestWithExpectedLatencyDiagnoserWithFixedReplicaScaling() {
    // given:
    kafkaStreamsPolicy.setSpec(
        specWithDiagnoser(DiagnoserSpec.expectedLatency(
            new ExpectedLatencyDiagnoserSpec(
                123,
                Optional.of(10),
                Optional.of(11),
                Optional.of(12),
                Optional.of(13),
                Optional.of(14),
                ScaleUpStrategySpec.fixedReplica(new FixedReplicaScaleUpStrategySpec(3))
            )
        ))
    );

    // when:
    final var request
        = ControllerProtoFactories.upsertPolicyRequest("", kafkaStreamsPolicy);

    // then:
    final var diagnoser = request.getPolicy().getKafkaStreamsPolicy().getDiagnoserList().get(0);
    assertThat(diagnoser.hasExpectedLatency(), is(true));
    assertThat(diagnoser.getExpectedLatency().getMaxExpectedLatencySeconds(), is(123));
    assertThat(diagnoser.getExpectedLatency().getWindowSeconds(), is(10));
    assertThat(diagnoser.getExpectedLatency().getProjectionSeconds(), is(11));
    assertThat(diagnoser.getExpectedLatency().getScaleDownBufferSeconds(), is(12));
    assertThat(diagnoser.getExpectedLatency().getGraceSeconds(), is(13));
    assertThat(diagnoser.getExpectedLatency().getStaggerSeconds(), is(14));
    assertThat(diagnoser.getExpectedLatency().hasFixedReplicas(), is(true));
    assertThat(diagnoser.getExpectedLatency().getFixedReplicas().getReplicas(), is(3));
  }

  @Test
  public void shouldCreateUpsertPolicyRequestWithExpectedLatencyDiagnoserWithRateBasedStrategy() {
    // given:
    kafkaStreamsPolicy.setSpec(
        specWithDiagnoser(DiagnoserSpec.expectedLatency(
            new ExpectedLatencyDiagnoserSpec(
                123,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ScaleUpStrategySpec.rateBased()
            )
        ))
    );

    // when:
    final var request
        = ControllerProtoFactories.upsertPolicyRequest("", kafkaStreamsPolicy);

    // then:
    final var diagnoser = request.getPolicy().getKafkaStreamsPolicy().getDiagnoserList().get(0);
    assertThat(diagnoser.hasExpectedLatency(), is(true));
    assertThat(diagnoser.getExpectedLatency().getMaxExpectedLatencySeconds(), is(123));
    assertThat(diagnoser.getExpectedLatency().hasRateBased(), is(true));
  }

  @Test
  public void shouldCreateUpsertPolicyRequestWithExpectedLatencyDiagnoserWithScaleToMaxStrategy() {
    // given:
    kafkaStreamsPolicy.setSpec(
        specWithDiagnoser(DiagnoserSpec.expectedLatency(
            new ExpectedLatencyDiagnoserSpec(
                123,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ScaleUpStrategySpec.scaleToMax()
            )
        ))
    );

    // when:
    final var request
        = ControllerProtoFactories.upsertPolicyRequest("", kafkaStreamsPolicy);

    // then:
    final var diagnoser = request.getPolicy().getKafkaStreamsPolicy().getDiagnoserList().get(0);
    assertThat(diagnoser.hasExpectedLatency(), is(true));
    assertThat(diagnoser.getExpectedLatency().getMaxExpectedLatencySeconds(), is(123));
    assertThat(diagnoser.getExpectedLatency().hasScaleToMax(), is(true));
  }

  @Test
  public void shouldCreateUpsertPolicyRequestWithExpectedLatencyDiagnoserWithEmptyOptionals() {
    // given:
    kafkaStreamsPolicy.setSpec(
        specWithDiagnoser(DiagnoserSpec.expectedLatency(
            new ExpectedLatencyDiagnoserSpec(
                123,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ScaleUpStrategySpec.scaleToMax()
            )
        ))
    );

    // when:
    final var request
        = ControllerProtoFactories.upsertPolicyRequest("", kafkaStreamsPolicy);

    // then:
    final var diagnoser = request.getPolicy().getKafkaStreamsPolicy().getDiagnoserList().get(0);
    assertThat(diagnoser.hasExpectedLatency(), is(true));
    assertThat(diagnoser.getExpectedLatency().hasWindowSeconds(), is(false));
    assertThat(diagnoser.getExpectedLatency().hasProjectionSeconds(), is(false));
    assertThat(diagnoser.getExpectedLatency().hasStaggerSeconds(), is(false));
    assertThat(diagnoser.getExpectedLatency().hasGraceSeconds(), is(false));
    assertThat(diagnoser.getExpectedLatency().hasScaleDownBufferSeconds(), is(false));
  }

  @Test
  public void shouldCreateCurrentStateRequestForDeployment() {
    // when:
    final var request = ControllerProtoFactories.currentStateRequest(
        TESTENV, kafkaStreamsPolicy, kafkaStreamsApplicationState);

    // Then:
    assertThat(request.getApplicationId(), is("testenv/gouda/cheddar"));
    assertThat(request.getState().hasKafkaStreamsState(), is(true));
    assertThat(request.getState().getKafkaStreamsState().getReplicas(), is(3));
  }

  @Test
  public void shouldSetAppIdInCurrentStateRequestWhenEnvEmpty() {
    // when:
    final var request = ControllerProtoFactories.currentStateRequest(
        "", kafkaStreamsPolicy, kafkaStreamsApplicationState);

    // Then:
    assertThat(request.getApplicationId(), is("gouda/cheddar"));
  }

  @Test
  public void shouldCreateEmptyRequest() {
    final var request = ControllerProtoFactories.emptyRequest(TESTENV, kafkaStreamsPolicy);

    assertThat(request.getApplicationId(), is("testenv/gouda/cheddar"));
  }

  @Test
  public void shouldCreateEmptyRequestWhenEnvEmpty() {
    final var request = ControllerProtoFactories.emptyRequest("", kafkaStreamsPolicy);

    assertThat(request.getApplicationId(), is("gouda/cheddar"));
  }
}
