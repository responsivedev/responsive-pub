package dev.responsive.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import dev.responsive.kafka.api.InternalConfigs;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StoreUtilTest {
  @Mock private Admin admin;

  @Test
  public void shouldThrowOnEnableAllOptimizations() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StoreUtil.validateTopologyOptimizationConfig(
                Map.of(
                    StreamsConfig.APPLICATION_ID_CONFIG, "foo",
                    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "foo.bar",
                    StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE)));
  }

  @Test
  public void shouldThrowOnEnableReuseSourceTopicOptimizations() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StoreUtil.validateTopologyOptimizationConfig(
                Map.of(
                    StreamsConfig.APPLICATION_ID_CONFIG, "foo",
                    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "foo.bar",
                    StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG,
                        StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS)));
  }

  @Test
  public void shouldNotThrowWhenOptimizationsOff() {
    StoreUtil.validateTopologyOptimizationConfig(
        Map.of(
            StreamsConfig.APPLICATION_ID_CONFIG, "foo",
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "foo.bar"));
  }

  @Test
  public void shouldNotTruncateChangelogIfCompacted() {
    // given:
    givenTopicCleanupPolicy("compacted-changelog", TopicConfig.CLEANUP_POLICY_COMPACT);
    final TopologyDescription description = givenTopologyWithTableSource("table-source");
    final Map<String, Object> configs =
        new InternalConfigs.Builder().withTopologyDescription(description).build();

    // when:
    final boolean compact =
        StoreUtil.shouldTruncateChangelog("compacted-changelog", admin, configs);

    // then:
    assertThat(compact, is(false));
  }

  @Test
  public void shouldNotTruncateChanglogIfSource() {
    // given:
    givenTopicCleanupPolicy("table-source", TopicConfig.CLEANUP_POLICY_DELETE);
    final TopologyDescription description = givenTopologyWithTableSource("table-source");
    final Map<String, Object> configs =
        new InternalConfigs.Builder().withTopologyDescription(description).build();

    // when:
    final boolean compact = StoreUtil.shouldTruncateChangelog("table-source", admin, configs);

    // then:
    assertThat(compact, is(false));
  }

  @Test
  public void shouldTruncateChangelogIfSafe() {
    // given:
    givenTopicCleanupPolicy("changelog", TopicConfig.CLEANUP_POLICY_DELETE);
    final TopologyDescription description = givenTopologyWithTableSource("table-source");
    final Map<String, Object> configs =
        new InternalConfigs.Builder().withTopologyDescription(description).build();

    // when:
    final boolean compact = StoreUtil.shouldTruncateChangelog("changelog", admin, configs);

    // then:
    assertThat(compact, is(true));
  }

  private void givenTopicCleanupPolicy(final String topic, final String policy) {
    givenTopicConfig(topic, TopicConfig.CLEANUP_POLICY_CONFIG, policy);
  }

  private void givenTopicConfig(final String topic, final String config, final String value) {
    final ConfigResource resource = new ConfigResource(Type.TOPIC, topic);
    final KafkaFutureImpl<Map<ConfigResource, Config>> configFuture = new KafkaFutureImpl<>();
    configFuture.complete(Map.of(resource, new Config(List.of(new ConfigEntry(config, value)))));
    final DescribeConfigsResult result = mock(DescribeConfigsResult.class);
    when(result.all()).thenReturn(configFuture);
    when(admin.describeConfigs(List.of(new ConfigResource(Type.TOPIC, topic)))).thenReturn(result);
  }

  private TopologyDescription givenTopologyWithTableSource(final String tableSourceTopic) {
    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<Long, Long> stream = builder.stream("stream-source");
    final KTable<Long, Long> table = builder.table(tableSourceTopic);
    stream.join(table, Long::sum);
    final KGroupedStream<Long, Long> grouped = stream.groupBy((k, v) -> v);
    grouped.count();
    final Properties props = new Properties();
    props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
    final Topology topology = builder.build(props);
    return topology.describe();
  }
}
