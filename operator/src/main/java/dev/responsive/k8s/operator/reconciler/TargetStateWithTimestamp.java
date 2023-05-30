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
import java.util.Objects;
import responsive.controller.v1.controller.proto.ControllerOuterClass;

class TargetStateWithTimestamp {
  private final Instant timestamp;
  private final ControllerOuterClass.ApplicationState targetState;

  TargetStateWithTimestamp(final ControllerOuterClass.ApplicationState targetState) {
    this(Instant.now(), targetState);
  }

  TargetStateWithTimestamp(final Instant timestamp,
                           final ControllerOuterClass.ApplicationState targetState) {
    this.timestamp = Objects.requireNonNull(timestamp);
    this.targetState = Objects.requireNonNull(targetState);
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public ControllerOuterClass.ApplicationState getTargetState() {
    return targetState;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TargetStateWithTimestamp that = (TargetStateWithTimestamp) o;
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
