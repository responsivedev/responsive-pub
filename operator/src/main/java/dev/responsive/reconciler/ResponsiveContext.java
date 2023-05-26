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

import dev.responsive.controller.ControllerClient;
import dev.responsive.k8s.crd.ResponsivePolicy;
import io.javaoperatorsdk.operator.processing.event.source.timer.TimerEventSource;
import java.util.Objects;

class ResponsiveContext {

  private final TimerEventSource<ResponsivePolicy> scheduler;
  private final ControllerClient controllerClient;

  ResponsiveContext(final ControllerClient controllerClient) {
    this(new TimerEventSource<>(), controllerClient);
  }

  public ResponsiveContext(
      final TimerEventSource<ResponsivePolicy> scheduler,
      final ControllerClient controllerClient
  ) {
    this.scheduler = Objects.requireNonNull(scheduler);
    this.controllerClient = Objects.requireNonNull(controllerClient);
  }

  public TimerEventSource<ResponsivePolicy> scheduler() {
    return scheduler;
  }

  public ControllerClient controllerClient() {
    return controllerClient;
  }
}
