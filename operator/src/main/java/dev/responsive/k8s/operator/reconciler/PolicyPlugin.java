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

import dev.responsive.k8s.crd.ResponsivePolicy;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import java.util.Map;

/** Interface for supporting various policy types operator-side */
public interface PolicyPlugin {
  Map<String, EventSource> prepareEventSources(
      EventSourceContext<ResponsivePolicy> ctx, ResponsiveContext responsiveCtx);

  void reconcile(
      ResponsivePolicy resource, Context<ResponsivePolicy> ctx, ResponsiveContext responsiveCtx);
}
