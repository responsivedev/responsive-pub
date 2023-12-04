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

package dev.responsive.kafka.internal.metrics.exporter.otel;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.internal.config.ConfigUtils;
import dev.responsive.kafka.internal.metrics.exporter.MetricsExportService;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.instrumentation.jmx.engine.JmxMetricInsight;
import io.opentelemetry.instrumentation.jmx.engine.MetricConfiguration;
import io.opentelemetry.instrumentation.jmx.yaml.JmxConfig;
import io.opentelemetry.instrumentation.jmx.yaml.JmxRule;
import io.opentelemetry.instrumentation.jmx.yaml.RuleParser;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.time.Duration;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import responsive.platform.auth.ApiKeyHeaders;

public class OtelMetricsService implements MetricsExportService  {

  private static final Logger LOG = LoggerFactory.getLogger(OtelMetricsService.class);

  private static final String SERVICE_NAME_ATTR = "service.name";
  private static final String RESPONSIVE_APPLICATION_ID_ATTR = "responsiveApplicationId";

  private final JmxMetricInsight metricInsight;
  private final OpenTelemetrySdk otel;

  public static OtelMetricsService create(
      final StreamsConfig streamsConfig,
      final ResponsiveConfig config
  ) {
    final OtlpGrpcMetricExporterBuilder builder = OtlpGrpcMetricExporter.builder();

    final String apiKey = config.getString(ResponsiveConfig.METRICS_API_KEY_CONFIG);
    final Password secret = config.getPassword(ResponsiveConfig.METRICS_SECRET_CONFIG);
    if (secret == null ^ apiKey == null) {
      throw new IllegalArgumentException(String.format(
          "Invalid configuration, if configured to report metrics using %s, "
              + "then values for both %s and %s must be provided.",
          ResponsiveConfig.METRICS_ENABLED_CONFIG,
          ResponsiveConfig.METRICS_API_KEY_CONFIG,
          ResponsiveConfig.METRICS_SECRET_CONFIG
      ));
    } else if (secret != null) {
      builder.addHeader(ApiKeyHeaders.API_KEY_METADATA_KEY, apiKey);
      builder.addHeader(ApiKeyHeaders.SECRET_METADATA_KEY, secret.value());
    }

    builder.setEndpoint(config.getString(ResponsiveConfig.CONTROLLER_ENDPOINT_CONFIG));

    final var exporter = builder.build();

    final var metricReader = PeriodicMetricReader
        .builder(exporter)
        .setInterval(Duration.ofSeconds(10))
        .build();

    final var appId = ConfigUtils.responsiveAppId(streamsConfig, config);
    final var attributes = Attributes
        .builder()
        .put(SERVICE_NAME_ATTR, appId + "-otel")
        .put(RESPONSIVE_APPLICATION_ID_ATTR, appId)
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

    return new OtelMetricsService(otel);
  }

  private OtelMetricsService(final OpenTelemetrySdk otel) {
    this.otel = otel;
    this.metricInsight = JmxMetricInsight.createService(otel, 0);
  }

  @Override
  public void start() {
    this.metricInsight.start(buildMetricConfiguration());
  }

  @Override
  public void close() {
    otel.close();
  }

  private static MetricConfiguration buildMetricConfiguration() {
    MetricConfiguration metricConfiguration = new MetricConfiguration();
    addRulesFromJmxConfig(metricConfiguration);
    return metricConfiguration;
  }

  private static void addRulesFromJmxConfig(MetricConfiguration conf) {
    final RuleParser parserInstance = RuleParser.get();
    final ClassLoader loader = OtelMetricsService.class.getClassLoader();

    // TODO(agavra): instead of including otel-jmx.config.yaml as a resource we should
    // fetch it from the Responsive controller on start-up
    try (final var inputStream = loader.getResourceAsStream("otel-jmx.config.yaml")) {
      final JmxConfig jmxConfig = parserInstance.loadConfig(inputStream);
      LOG.info("Found {} metric rules", jmxConfig.getRules().size());

      for (final JmxRule rule : jmxConfig.getRules()) {
        conf.addMetricDef(rule.buildMetricDef());
      }
    } catch (final Exception e) {
      LOG.error("Unable to load rules from otel-jmx.config.yaml!", e);
      throw new IllegalStateException("Unable to load rules from otel-jmx.config.yaml.", e);
    }
  }
}
