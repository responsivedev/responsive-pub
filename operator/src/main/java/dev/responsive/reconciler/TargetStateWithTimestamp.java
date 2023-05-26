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

package dev.responsive.reconciler;

import java.time.Instant;
import java.util.Objects;
import responsive.controller.v1.controller.proto.ControllerOuterClass;
import responsive.controller.v1.controller.proto.ControllerOuterClass.ApplicationState;

class TargetStateWithTimestamp {

  private final Instant timestamp;
  private final ControllerOuterClass.ApplicationState targetState;

  TargetStateWithTimestamp(final ControllerOuterClass.ApplicationState targetState) {
    this(Instant.now(), targetState);
  }

  public TargetStateWithTimestamp(final Instant timestamp, final ApplicationState targetState) {
    this.timestamp = Objects.requireNonNull(timestamp);
    this.targetState = Objects.requireNonNull(targetState);
  }

  public Instant timestamp() {
    return timestamp;
  }

  public ApplicationState targetState() {
    return targetState;
  }
}
