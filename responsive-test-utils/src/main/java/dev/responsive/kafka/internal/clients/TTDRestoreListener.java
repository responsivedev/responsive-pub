/*
 *
 *  * Copyright 2023 Responsive Computing, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package dev.responsive.kafka.internal.clients;

import dev.responsive.kafka.internal.metrics.ClientVersionMetadata;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.metrics.ResponsiveRestoreListener;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.StreamsConfig;

public class TTDRestoreListener extends ResponsiveRestoreListener {

  private final ResponsiveMetrics metrics;

  public TTDRestoreListener(final ResponsiveMetrics metrics) {
    super(metrics);
    this.metrics = metrics;
  }

  public static TTDRestoreListener mockRestoreListener(final Properties props) {
    final String appId = props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
    final var metrics = new ResponsiveMetrics(new Metrics());
    metrics.initializeTags(
        appId,
        appId + "-client",
        ClientVersionMetadata.loadVersionMetadata(),
        Collections.emptyMap()
    );
    return new TTDRestoreListener(metrics);
  }

  public ResponsiveMetrics metrics() {
    return metrics;
  }

}
