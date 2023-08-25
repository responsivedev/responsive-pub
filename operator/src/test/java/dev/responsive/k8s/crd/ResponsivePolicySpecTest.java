package dev.responsive.k8s.crd;

import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.responsive.k8s.crd.ResponsivePolicySpec.PolicyType;
import dev.responsive.k8s.crd.kafkastreams.DemoPolicySpec;
import dev.responsive.k8s.crd.kafkastreams.DiagnoserSpec;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import responsive.controller.v1.controller.proto.ControllerOuterClass.PolicyStatus;

class ResponsivePolicySpecTest {

  @Test
  public void shouldThrowOnNullName() {
    // given:
    final var spec = new ResponsivePolicySpec(
        "foo",
        null,
        PolicyStatus.POLICY_STATUS_MANAGED,
        PolicyType.DEMO,
        Optional.of(new DemoPolicySpec(
            10,
            0,
            1,
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
        PolicyStatus.POLICY_STATUS_MANAGED,
        PolicyType.DEMO,
        Optional.of(new DemoPolicySpec(
            10,
            0,
            1,
            Optional.empty()
        )),
        Optional.empty()
    );

    // when/then:
    assertThrows(NullPointerException.class, spec::validate);
  }

  @Test
  public void shouldThrowOnNullStatus() {
    // given:
    final var spec = new ResponsivePolicySpec(
        "baz",
        "foo",
        null,
        PolicyType.DEMO,
        Optional.of(new DemoPolicySpec(
            10,
            0,
            1,
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
        PolicyStatus.POLICY_STATUS_MANAGED,
        null,
        Optional.of(new DemoPolicySpec(
            10,
            0,
            1,
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
                    Optional.empty()
                )
            )))),
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
        PolicyStatus.POLICY_STATUS_MANAGED,
        PolicyType.DEMO,
        Optional.of(new DemoPolicySpec(
            10,
            0,
            1,
            Optional.of(List.of(DiagnoserSpec.lag())))),
        Optional.empty()
    );

    // when/then:
    spec.validate();
  }
}