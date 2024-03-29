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

package dev.responsive.kafka.internal.config;

import static dev.responsive.kafka.internal.config.ResponsiveStreamsConfig.verifyNoStandbys;
import static dev.responsive.kafka.internal.config.ResponsiveStreamsConfig.verifyNotEosV1;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ResponsiveStreamsConfigTest {

  @Test
  public void shouldThrowOnNonZeroNumStandbyReplicas() {
    assertThrows(
        ConfigException.class,
        () -> verifyNoStandbys(new StreamsConfig(Map.of(
            StreamsConfig.APPLICATION_ID_CONFIG, "foo",
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "foo.bar",
            StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1
        )))
    );
  }

  @Test
  public void shouldNotThrowWhenNumStandbysUnset() {
    verifyNoStandbys(new StreamsConfig(Map.of(
        StreamsConfig.APPLICATION_ID_CONFIG, "foo",
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "foo.bar"
    )));
  }

  @Test
  public void shouldNotThrowWhenNumStandbysSetToZero() {
    verifyNoStandbys(new StreamsConfig(Map.of(
        StreamsConfig.APPLICATION_ID_CONFIG, "foo",
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "foo.bar",
        StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0
    )));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void shouldThrowOnEOSV1() {
    assertThrows(
        ConfigException.class,
        () -> verifyNotEosV1(new StreamsConfig(Map.of(
            StreamsConfig.APPLICATION_ID_CONFIG, "foo",
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "foo.bar",
            StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE
        )))
    );
  }
}
