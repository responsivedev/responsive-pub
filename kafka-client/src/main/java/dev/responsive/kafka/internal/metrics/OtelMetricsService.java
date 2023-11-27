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

package dev.responsive.kafka.internal.metrics;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.config.ConfigUtils;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.instrumentation.jmx.engine.JmxMetricInsight;
import io.opentelemetry.instrumentation.jmx.engine.MetricConfiguration;
import io.opentelemetry.instrumentation.jmx.yaml.RuleParser;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.io.Closeable;
import java.time.Duration;
import org.apache.kafka.streams.StreamsConfig;

public class OtelMetricsService implements Closeable {

  private final JmxMetricInsight metricInsight;
  private final Runnable onClose;

  public static OtelMetricsService create(
      final StreamsConfig streamsConfig,
      final ResponsiveConfig config
  ) {
    final var exporter = OtlpGrpcMetricExporter
        .builder()
        .addHeader("api-key", config.getString(ResponsiveConfig.METRICS_API_KEY_CONFIG))
        .addHeader("secret", config.getString(ResponsiveConfig.METRICS_API_KEY_CONFIG))
        .setEndpoint(config.getString(ResponsiveConfig.CONTROLLER_ENDPOINT_CONFIG))
        .build();

    final var metricReader = PeriodicMetricReader
        .builder(exporter)
        .setInterval(Duration.ofSeconds(10))
        .build();

    final var appId = ConfigUtils.responsiveAppId(streamsConfig, config);
    final var attributes = Attributes
        .builder()
        .put("service.name", appId + "-otel")
        .put("responsiveApplicationId", appId)
        .build();

    final var meterProvider = SdkMeterProvider
        .builder()
        .setResource(Resource.create(attributes))
        .registerMetricReader(metricReader)
        .build();

    final OpenTelemetrySdk otel = OpenTelemetrySdk
        .builder()
        .setMeterProvider(meterProvider)
        .setPropagators(ContextPropagators.create(TextMapPropagator.composite(
            W3CTraceContextPropagator.getInstance(),
            W3CBaggagePropagator.getInstance()
        )))
        .build();

    return new OtelMetricsService(otel, otel::close);
  }

  public static OtelMetricsService noop() {
    return new OtelMetricsService(OpenTelemetry.noop(), () -> {});
  }

  private OtelMetricsService(final OpenTelemetry otel, final Runnable onClose) {
    this.onClose = onClose;
    this.metricInsight = JmxMetricInsight.createService(otel, 0);
  }

  public void start() {
    this.metricInsight.start(buildMetricConfiguration());
  }

  @Override
  public void close() {
    onClose.run();
  }

  private static void buildFromUserRules(MetricConfiguration conf) {
    RuleParser parserInstance = RuleParser.get();
    final ClassLoader loader = OtelMetricsService.class.getClassLoader();

    // TODO(agavra): instead of including otel-jmx.config.yaml as a resource we should
    // fetch it from the Responsive controller on start-up
    try (final var is = loader.getResourceAsStream("otel-jmx.config.yaml")) {
      parserInstance.addMetricDefsTo(conf, is, "responsive builtin config");
    } catch (Exception e) {
      // yaml parsing errors are caught and logged inside addMetricDefsTo
      // only file access related exceptions are expected here
      JmxMetricInsight.getLogger().warning(e.toString());
    }
  }

  private static MetricConfiguration buildMetricConfiguration() {
    MetricConfiguration metricConfiguration = new MetricConfiguration();
    buildFromUserRules(metricConfiguration);
    return metricConfiguration;
  }
}
