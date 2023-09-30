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

package dev.responsive.internal.config;

import static dev.responsive.internal.config.ResponsiveStreamsConfig.verifyNoStandbys;
import static dev.responsive.internal.config.ResponsiveStreamsConfig.verifyNotEosV1;
import static dev.responsive.internal.config.ResponsiveStreamsConfig.verifyTopologyOptimizationConfig;
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

  @Test
  public void shouldNotThrowOnEOSV2() {
    verifyTopologyOptimizationConfig(new StreamsConfig(Map.of(
        StreamsConfig.APPLICATION_ID_CONFIG, "foo",
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "foo.bar",
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2
    )));
  }

  @Test
  public void shouldNotThrowOnALOS() {
    verifyTopologyOptimizationConfig(new StreamsConfig(Map.of(
        StreamsConfig.APPLICATION_ID_CONFIG, "foo",
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "foo.bar",
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE
    )));
  }
  
  @Test
  public void shouldThrowOnEnableAllOptimizations() {
    assertThrows(
        ConfigException.class,
        () -> verifyTopologyOptimizationConfig(new StreamsConfig(Map.of(
            StreamsConfig.APPLICATION_ID_CONFIG, "foo",
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "foo.bar",
            StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE
        )))
    );
  }

  @Test
  public void shouldThrowOnEnableReuseSourceTopicOptimizations() {
    assertThrows(
        ConfigException.class,
        () -> verifyTopologyOptimizationConfig(new StreamsConfig(Map.of(
            StreamsConfig.APPLICATION_ID_CONFIG, "foo",
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "foo.bar",
            StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS
        )))
    );
  }

  @Test
  public void shouldThrowOnEnableReuseSourceTopicMultiOptimization() {
    assertThrows(
        ConfigException.class,
        () -> verifyTopologyOptimizationConfig(new StreamsConfig(Map.of(
            StreamsConfig.APPLICATION_ID_CONFIG, "foo",
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "foo.bar",
            StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG,
            StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS + "," + StreamsConfig.MERGE_REPARTITION_TOPICS
        )))
    );
  }

  @Test
  public void shouldNotThrowWhenOptimizationsOff() {
    verifyTopologyOptimizationConfig(new StreamsConfig(Map.of(
        StreamsConfig.APPLICATION_ID_CONFIG, "foo",
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "foo.bar")
    ));
  }

  @Test
  public void shouldNotThrowWhenOptimizationsDoNotIncludeReuseSourceTopic() {
    verifyTopologyOptimizationConfig(new StreamsConfig(Map.of(
        StreamsConfig.APPLICATION_ID_CONFIG, "foo",
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "foo.bar",
        StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG,
        StreamsConfig.SINGLE_STORE_SELF_JOIN + "," + StreamsConfig.MERGE_REPARTITION_TOPICS)
    ));
  }
}
