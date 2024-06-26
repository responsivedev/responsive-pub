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

package dev.responsive.k8s.operator.reconciler;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import responsive.controller.v1.controller.proto.ControllerOuterClass;

class ActionsWithTimestamp {
  private final Instant timestamp;
  private final Optional<ControllerOuterClass.ApplicationState> targetState;
  private final List<ControllerOuterClass.Action> actions;

  ActionsWithTimestamp(
      final Optional<ControllerOuterClass.ApplicationState> targetState,
      final List<ControllerOuterClass.Action> actions
  ) {
    this(Instant.now(), targetState, actions);
  }

  ActionsWithTimestamp(final List<ControllerOuterClass.Action> actions) {
    this(Instant.now(), Optional.empty(), actions);
  }

  ActionsWithTimestamp() {
    this(Instant.now(), Optional.empty(), Collections.emptyList());
  }

  ActionsWithTimestamp(final Instant timestamp,
                       final Optional<ControllerOuterClass.ApplicationState> targetState,
                       final List<ControllerOuterClass.Action> actions) {
    this.timestamp = Objects.requireNonNull(timestamp);
    this.targetState = Objects.requireNonNull(targetState);
    this.actions = List.copyOf(Objects.requireNonNull(actions));
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public Optional<ControllerOuterClass.ApplicationState> getTargetState() {
    return targetState;
  }

  public List<ControllerOuterClass.Action> getActions() {
    return actions;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ActionsWithTimestamp that = (ActionsWithTimestamp) o;
    return Objects.equals(timestamp, that.timestamp)
        && Objects.equals(targetState, that.targetState);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp, targetState);
  }

  @Override
  public String toString() {
    return "TargetStateWithTimestamp{"
        + "timestamp=" + timestamp
        + ", targetState=" + targetState
        + '}';
  }
}
