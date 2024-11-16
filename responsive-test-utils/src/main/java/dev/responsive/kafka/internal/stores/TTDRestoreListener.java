/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
 */

package dev.responsive.kafka.internal.stores;

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
