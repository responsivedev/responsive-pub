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

import dev.responsive.k8s.crd.ResponsivePolicy;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import java.util.Map;

/**
 * Interface for supporting various policy types operator-side
 */
public interface PolicyPlugin {
  Map<String, EventSource> prepareEventSources(EventSourceContext<ResponsivePolicy> ctx,
                                               ResponsiveContext responsiveCtx);

  void reconcile(
      ResponsivePolicy resource, Context<ResponsivePolicy> ctx, ResponsiveContext responsiveCtx);
}
