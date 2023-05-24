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

package dev.responsive;

import org.testcontainers.utility.DockerImageName;

public class TestConstants {

  public static final DockerImageName CASSANDRA = DockerImageName.parse("cassandra:4.1.0");
  public static final DockerImageName KAFKA = DockerImageName.parse("confluentinc/cp-kafka:7.3.2");

}