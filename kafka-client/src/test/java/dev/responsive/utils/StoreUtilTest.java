package dev.responsive.utils;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StoreUtilTest {

  @Test
  public void shouldThrowOnEnableAllOptimizations() {
    assertThrows(
        IllegalArgumentException.class,
        () -> StoreUtil.validateTopologyOptimizationConfig(new StreamsConfig(Map.of(
            StreamsConfig.APPLICATION_ID_CONFIG, "foo",
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "foo.bar",
            StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE

        )))
    );
  }

  @Test
  public void shouldThrowOnEnableReuseSourceTopicOptimizations() {
    assertThrows(
        IllegalArgumentException.class,
        () -> StoreUtil.validateTopologyOptimizationConfig(new StreamsConfig(Map.of(
            StreamsConfig.APPLICATION_ID_CONFIG, "foo",
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "foo.bar",
            StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS

        )))
    );
  }

  @Test
  public void shouldThrowOnEnableReuseSourceTopicMultiOptimization() {
    assertThrows(
        IllegalArgumentException.class,
        () -> StoreUtil.validateTopologyOptimizationConfig(new StreamsConfig(Map.of(
            StreamsConfig.APPLICATION_ID_CONFIG, "foo",
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "foo.bar",
            StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG,
            StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS + "," + StreamsConfig.MERGE_REPARTITION_TOPICS

        )))
    );
  }

  @Test
  public void shouldNotThrowWhenOptimizationsOff() {
    StoreUtil.validateTopologyOptimizationConfig(new StreamsConfig(Map.of(
        StreamsConfig.APPLICATION_ID_CONFIG, "foo",
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "foo.bar")
    ));
  }

  @Test
  public void shouldNotThrowWhenOptimizationsDoNotIncludeReuseSourceTopic() {
    StoreUtil.validateTopologyOptimizationConfig(new StreamsConfig(Map.of(
        StreamsConfig.APPLICATION_ID_CONFIG, "foo",
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "foo.bar",
        StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG,
        StreamsConfig.SINGLE_STORE_SELF_JOIN + "," + StreamsConfig.MERGE_REPARTITION_TOPICS)
    ));
  }

}