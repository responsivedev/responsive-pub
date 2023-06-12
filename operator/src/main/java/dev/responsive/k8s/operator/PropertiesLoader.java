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

package dev.responsive.k8s.operator;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads {@link java.util.Properties} depending on the encoding
 * of the file type.
 */
public final class PropertiesLoader {

  private static final Logger LOG = LoggerFactory.getLogger(PropertiesLoader.class);

  private PropertiesLoader() {}

  public static Properties load(final String fileName) {
    final Properties properties = new Properties();
    try {
      final Reader reader;
      if (fileName.endsWith("b64")) {
        final byte[] encodedBytes = Files.readAllBytes(Paths.get(fileName));
        byte[] decodedBytes = Base64.getDecoder().decode(encodedBytes);
        reader = new StringReader(new String(decodedBytes));
      } else {
        final File configFile = new File(fileName);
        reader = new FileReader(configFile);
      }
      properties.load(reader);
    } catch (Exception e) {
      LOG.error("Error loading configuration properties: {}", e.getMessage());
    }
    return properties;
  }

}
