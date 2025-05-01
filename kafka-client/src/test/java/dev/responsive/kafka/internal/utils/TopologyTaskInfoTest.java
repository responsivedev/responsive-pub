package dev.responsive.kafka.internal.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TopologyTaskInfoTest {
  @Mock
  private Admin admin;

  @SuppressWarnings("unchecked")
  @BeforeEach
  public void setup() {
    when(admin.describeTopics(any(Collection.class))).thenAnswer(
        i -> {
          final var topics = i.<Collection<String>>getArgument(0);
          final Map<String, KafkaFuture<TopicDescription>> futures = topics.stream()
              .collect(Collectors.toMap(
                  t -> t,
                  t -> KafkaFuture.completedFuture(new TopicDescription(
                      t,
                      false,
                      List.of(
                          new TopicPartitionInfo(0, null, List.of(), List.of()),
                          new TopicPartitionInfo(1, null, List.of(), List.of())
                      )
                  ))
              ));
          return new TestDescribeTopicsResult(futures);
        }
    );
  }

  @Test
  public void shouldComputeTaskAndPartitionMappings() {
    // given:
    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream("source", Consumed.with(Serdes.Long(), Serdes.Long()))
        .groupByKey(Grouped.as("groupBy"))
        .count(Named.as("count"))
        .toStream(Named.as("toStream"))
        .to("sink");
    final TopologyDescription description = builder.build().describe();

    // when:
    final var tti = TopologyTaskInfo.forTopology(description, admin);

    // then:
    assertThat(
        tti.partitionsByTask(),
        is(
            Map.of(
                new TaskId(0, 0), List.of(new TopicPartition("source", 0)),
                new TaskId(0, 1), List.of(new TopicPartition("source", 1))
            )
        )
    );
    assertThat(
        tti.tasksByPartition(),
        is(
            Map.of(
                new TopicPartition("source", 0), new TaskId(0, 0),
                new TopicPartition("source", 1), new TaskId(0, 1)
            )
        )
    );
  }

  @Test
  public void shouldThrowIfRepartition() {
    // given:
    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream("source", Consumed.with(Serdes.Long(), Serdes.Long()))
        .groupBy((k, v) -> v, Grouped.as("groupBy"))
        .count(Named.as("count"))
        .toStream(Named.as("toStream"))
        .to("sink");
    final TopologyDescription description = builder.build().describe();

    // when/then:
    assertThrows(
        TopologyTaskInfo.TopologyTaskInfoException.class,
        () -> TopologyTaskInfo.forTopology(description, admin)
    );
  }

  private static class TestDescribeTopicsResult extends DescribeTopicsResult {
    protected TestDescribeTopicsResult(
        final Map<String, KafkaFuture<TopicDescription>> nameFutures
    ) {
      super(null, nameFutures);
    }
  }
}
