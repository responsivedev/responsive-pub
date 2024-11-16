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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.responsive.k8s.crd.ResponsivePolicySpec.PolicyType;
import dev.responsive.k8s.crd.kafkastreams.DemoPolicySpec;
import dev.responsive.k8s.crd.kafkastreams.DiagnoserSpec;
import java.util.List;
import java.util.Optional;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import responsive.controller.v1.controller.proto.ControllerOuterClass.PolicyStatus;

class ResponsivePolicySpecTest {

  @Test
  public void shouldThrowOnNullName() {
    // given:
    final var spec = new ResponsivePolicySpec(
        "foo",
        null,
        "bar",
        PolicyStatus.POLICY_STATUS_MANAGED,
        PolicyType.DEMO,
        Optional.of(new DemoPolicySpec(
            10,
            0,
            1,
            Optional.empty(),
            Optional.empty()
        )),
        Optional.empty()
    );

    // when/then:
    assertThrows(NullPointerException.class, spec::validate);
  }

  @Test
  public void shouldThrowOnNullNamespace() {
    // given:
    final var spec = new ResponsivePolicySpec(
        null,
        "foo",
        "bar",
        PolicyStatus.POLICY_STATUS_MANAGED,
        PolicyType.DEMO,
        Optional.of(new DemoPolicySpec(
            10,
            0,
            1,
            Optional.empty(),
            Optional.empty()
        )),
        Optional.empty()
    );

    // when/then:
    assertThrows(NullPointerException.class, spec::validate);
  }

  @Test
  public void shouldFillInNullAppId() {
    // given:
    final var spec = new ResponsivePolicySpec(
        "baz",
        "foo",
        null,
        PolicyStatus.POLICY_STATUS_MANAGED,
        PolicyType.DEMO,
        Optional.of(new DemoPolicySpec(
            10,
            0,
            1,
            Optional.empty(),
            Optional.empty()
        )),
        Optional.empty()
    );

    // When:
    assertThat(spec.getApplicationId(), Matchers.is("baz/foo"));
  }

  @Test
  public void shouldThrowOnNullStatus() {
    // given:
    final var spec = new ResponsivePolicySpec(
        "baz",
        "foo",
        "bar",
        null,
        PolicyType.DEMO,
        Optional.of(new DemoPolicySpec(
            10,
            0,
            1,
            Optional.empty(),
            Optional.empty()
        )),
        Optional.empty()
    );

    // when/then:
    assertThrows(NullPointerException.class, spec::validate);
  }

  @Test
  public void shouldThrowOnNullType() {
    // given:
    final var spec = new ResponsivePolicySpec(
        "baz",
        "foo",
        "bar",
        PolicyStatus.POLICY_STATUS_MANAGED,
        null,
        Optional.of(new DemoPolicySpec(
            10,
            0,
            1,
            Optional.empty(),
            Optional.empty())),
        Optional.empty()
    );

    // when/then:
    assertThrows(NullPointerException.class, spec::validate);
  }

  @Test
  public void shouldThrowOnNullDiagnoserType() {
    // given:
    final var spec = new ResponsivePolicySpec(
        "baz",
        "foo",
        "bar",
        PolicyStatus.POLICY_STATUS_MANAGED,
        PolicyType.DEMO,
        Optional.of(new DemoPolicySpec(
            10,
            0,
            1,
            Optional.of(List.of(
                new DiagnoserSpec(
                    null,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()
                )
            )),
            Optional.empty()
        )),
        Optional.empty()
    );

    // when/then:
    assertThrows(NullPointerException.class, spec::validate);
  }

  @Test
  public void shouldNotThrowOnValid() {
    // given:
    final var spec = new ResponsivePolicySpec(
        "baz",
        "foo",
        "bar",
        PolicyStatus.POLICY_STATUS_MANAGED,
        PolicyType.DEMO,
        Optional.of(new DemoPolicySpec(
            10,
            0,
            1,
            Optional.of(List.of(DiagnoserSpec.lag())),
            Optional.empty()
        )),
        Optional.empty()
    );

    // when/then:
    spec.validate();
  }

  @Test
  public void shouldNotThrowOnValidCooldown() {
    // given:
    final var spec = new ResponsivePolicySpec(
        "baz",
        "foo",
        "bar",
        PolicyStatus.POLICY_STATUS_MANAGED,
        PolicyType.DEMO,
        Optional.of(new DemoPolicySpec(
            10,
            0,
            1,
            Optional.of(List.of(DiagnoserSpec.lag())),
            Optional.of(new PolicyCooldownSpec(
                Optional.of(100),
                Optional.empty()
            ))
        )),
        Optional.empty()
    );

    // when/then:
    spec.validate();
  }

  @Test
  public void shouldThrowOnInvalidCooldown() {
    // given:
    final var spec = new ResponsivePolicySpec(
        "baz",
        "foo",
        "bar",
        PolicyStatus.POLICY_STATUS_MANAGED,
        PolicyType.DEMO,
        Optional.of(new DemoPolicySpec(
            10,
            0,
            1,
            Optional.of(List.of(DiagnoserSpec.lag())),
            Optional.of(new PolicyCooldownSpec(
                Optional.of(-100),
                Optional.empty()
            ))
        )),
        Optional.empty()
    );

    // when/then:
    assertThrows(RuntimeException.class, spec::validate);
  }
}