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

import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Properties;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

public class PropertiesLoaderTest {

  @Test
  public void shouldDecodeB64Properties() {
    // Given:
    final String path = PropertiesLoaderTest.class.getResource("/sample.properties.b64").getPath();

    // When:
    final Properties properties = PropertiesLoader.load(path);

    // Then:
    assertThat(properties.getProperty("foo.bar"), Matchers.is("baz"));
  }

  @Test
  public void shouldLoadNonB64Properties() {
    // Given:
    final String path = PropertiesLoaderTest.class.getResource("/sample.properties").getPath();

    // When:
    final Properties properties = PropertiesLoader.load(path);

    // Then:
    assertThat(properties.getProperty("foo.bar"), Matchers.is("baz"));
  }

}