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

package dev.responsive.k8s.crd;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Optional;

public class PolicyCooldownSpec {

  private static final Integer MINIMUM_STATE_TRANSITION_COOLDOWN_SECONDS = 0;
  private static final Integer MINIMUM_REBALANCE_COOLDOWN_SECONDS = 0;

  private final Optional<Integer> stateTransitionCooldownSeconds;
  private final Optional<Integer> rebalanceCooldownSeconds;

  @JsonCreator
  public PolicyCooldownSpec(
      @JsonProperty("stateTransitionCooldownSeconds")
      final Optional<Integer> stateTransitionCooldownSeconds,
      @JsonProperty("rebalanceCooldownSeconds") final Optional<Integer> rebalanceCooldownSeconds
  ) {
    this.stateTransitionCooldownSeconds = stateTransitionCooldownSeconds;
    this.rebalanceCooldownSeconds = rebalanceCooldownSeconds;
  }

  public Optional<Integer> getStateTransitionCooldownSeconds() {
    return stateTransitionCooldownSeconds;
  }

  public Optional<Integer> getRebalanceCooldownSeconds() {
    return rebalanceCooldownSeconds;
  }

  public void validate() {
    if (stateTransitionCooldownSeconds.isPresent()
        && stateTransitionCooldownSeconds.get() < MINIMUM_STATE_TRANSITION_COOLDOWN_SECONDS) {
      throw new RuntimeException(String.format(
          "stateTransitionCooldownSeconds of %d should be greater than %d",
          stateTransitionCooldownSeconds.get(), MINIMUM_STATE_TRANSITION_COOLDOWN_SECONDS
      ));
    }

    if (rebalanceCooldownSeconds.isPresent()
        && rebalanceCooldownSeconds.get() < MINIMUM_REBALANCE_COOLDOWN_SECONDS) {
      throw new RuntimeException(String.format(
          "rebalanceCooldownSeconds of %d should be greater than %d",
          rebalanceCooldownSeconds.get(), MINIMUM_REBALANCE_COOLDOWN_SECONDS
      ));
    }
  }
}
