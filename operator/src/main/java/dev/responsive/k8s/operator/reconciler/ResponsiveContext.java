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

package dev.responsive.k8s.operator.reconciler;

import dev.responsive.controller.client.ControllerClient;
import dev.responsive.k8s.crd.ResponsivePolicy;
import io.javaoperatorsdk.operator.processing.event.source.timer.TimerEventSource;
import java.util.Objects;

/**
 * POJO encapsulating all the operator context to share w/ plugins
 */
final class ResponsiveContext {
  final TimerEventSource<ResponsivePolicy> scheduler;
  final ControllerClient controllerClient;

  ResponsiveContext(final ControllerClient controllerClient) {
    this(new TimerEventSource<>(), controllerClient);
  }

  ResponsiveContext(
      final TimerEventSource<ResponsivePolicy> scheduler,
      final ControllerClient controllerClient
  ) {
    this.scheduler = Objects.requireNonNull(scheduler);
    this.controllerClient = Objects.requireNonNull(controllerClient);
  }

  public TimerEventSource<ResponsivePolicy> getScheduler() {
    return scheduler;
  }

  public ControllerClient getControllerClient() {
    return controllerClient;
  }
}
