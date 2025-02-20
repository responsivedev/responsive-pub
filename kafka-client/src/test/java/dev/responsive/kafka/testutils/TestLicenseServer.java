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

package dev.responsive.kafka.testutils;

import com.sun.net.httpserver.HttpServer;
import dev.responsive.kafka.internal.license.server.model.OriginEventsReportRequestV1;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

public class TestLicenseServer implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(TestLicenseServer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Map<String, Long> eventCounts = new ConcurrentHashMap<>();
  private int port;
  private HttpServer server;

  public TestLicenseServer() {
  }

  public synchronized void start() throws IOException {
    LOG.info("Starting test license server...");
    // Create a server on a random available port (port 0 means OS-assigned)
    server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/usage", exchange -> {
      final var body = exchange.getRequestBody();
      final var req = MAPPER.readValue(body, OriginEventsReportRequestV1.class);

      eventCounts.compute(
          req.applicationId(),
          (k, v) -> v == null ? req.eventCount() : v + req.eventCount()
      );

      String response = "OK";
      exchange.sendResponseHeaders(200, response.getBytes().length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(response.getBytes());
      }
    });
    server.setExecutor(Executors.newSingleThreadExecutor());
    server.start();

    // Capture the actual port assigned
    port = server.getAddress().getPort();
    LOG.info("Started test license server on port {}", port);
  }

  @Override
  public synchronized void close() {
    LOG.info("Stopping test license server.");
    if (server != null) {
      server.stop(0);
    }
  }

  public int port() {
    if (server == null) {
      throw new IllegalStateException("Cannot call port() before starting server");
    }
    return port;
  }

  public Map<String, Long> eventCounts() {
    return eventCounts;
  }
}
