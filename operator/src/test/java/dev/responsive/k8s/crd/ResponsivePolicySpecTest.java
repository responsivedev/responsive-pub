package dev.responsive.k8s.crd;

import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.responsive.k8s.crd.DemoPolicy.Diagnoser;
import dev.responsive.k8s.crd.ResponsivePolicySpec.PolicyType;
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
        Optional.of(new DemoPolicy(10, 0, Optional.empty()))
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
        Optional.of(new DemoPolicy(10, 0, Optional.empty()))
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
        Optional.of(new DemoPolicy(10, 0, Optional.empty()))
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
        Optional.of(new DemoPolicy(10, 0, Optional.empty()))
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
        Optional.of(new DemoPolicy(
            10,
            0,
            Optional.of(List.of(
                new Diagnoser(null, Optional.empty(), Optional.empty())
            ))))
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
        Optional.of(new DemoPolicy(
            10,
            0,
            Optional.of(List.of(Diagnoser.lag()))))
    );

    // when/then:
    spec.validate();
  }
}