package dev.responsive.examples.simpleapp;

import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.semconv.ResourceAttributes;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class Main {
  public static void main(String[] args) throws IOException {
    final Map<Object, Object> rawCfg;
    try (InputStream input = new FileInputStream(args[0])) {
      final Properties properties = new Properties();
      properties.load(input);
      rawCfg = properties.keySet().stream()
          .collect(Collectors.toMap(k -> k, properties::get));
    }

    SpanExporter otlpExporter = OtlpGrpcSpanExporter.builder()
        .setEndpoint("http://localhost:4317")  // Replace with your OTLP collector endpoint
        .build();
    SpanProcessor spanProcessor = SimpleSpanProcessor.create(otlpExporter);
    SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
        .addSpanProcessor(spanProcessor)
        .setResource(Resource.getDefault().toBuilder()
            .put(ResourceAttributes.SERVICE_NAME, "rohan-test-2")
            .build())
        .build();
    OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
        .setTracerProvider(sdkTracerProvider)
        .buildAndRegisterGlobal();

    final SimpleApplication application = new SimpleApplication(rawCfg);
    Runtime.getRuntime().addShutdownHook(new Thread(application::stop));
    application.start();
    application.await();
  }
}