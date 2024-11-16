/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Software License Agreement v1.0 available at
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev/sdk/get-started.
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
