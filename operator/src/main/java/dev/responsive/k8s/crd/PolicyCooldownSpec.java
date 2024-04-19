/*
 *  Copyright 2024 Responsive Computing, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
