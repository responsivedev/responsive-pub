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

package dev.responsive.examples.simpleapp;

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
    final SimpleApplication application = new SimpleApplication(rawCfg);
    Runtime.getRuntime().addShutdownHook(new Thread(application::stop));
    application.start();
    application.await();
  }
}