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

package dev.responsive.kafka.internal.clients;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import dev.responsive.kafka.internal.clients.ResponsiveGlobalConsumer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ResponsiveGlobalConsumerTest {

  @Mock
  private Consumer<byte[], byte[]> delegate;
  @Mock
  private Admin admin;

  private ResponsiveGlobalConsumer consumer;

  @BeforeEach
  public void before() {
    consumer = new ResponsiveGlobalConsumer(
        Map.of(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class
        ),
        delegate,
        admin
    );
  }

  @Test
  public void shouldInterceptCallsToSeek() {
    // When:
    consumer.seek(null, 0);
    consumer.seekToBeginning(null);
    consumer.seekToEnd(null);
    consumer.seek(null, new OffsetAndMetadata(0));

    // Then:
    verifyNoInteractions(delegate);
  }

  @Test
  public void shouldSubscribeToAssignedTopicsInsteadOfCallingAssign() {
    // When:
    consumer.assign(List.of(new TopicPartition("foo", 0), new TopicPartition("bar", 1)));

    // Then:
    verify(delegate, never()).assign(any());
    verify(delegate).subscribe(Set.of("foo", "bar"));
  }

  @Test
  public void shouldReturnAllRecordsForTopicOnPollResultOnSingleTopicPartition() {
    // Given:
    final byte[] b = new byte[]{};
    final ConsumerRecord<byte[], byte[]> fooRecord0 = new ConsumerRecord<>("foo", 0, 0, b, b);
    final ConsumerRecord<byte[], byte[]> fooRecord1 = new ConsumerRecord<>("foo", 0, 0, b, b);
    final ConsumerRecord<byte[], byte[]> barRecord = new ConsumerRecord<>("bar", 1, 0, b, b);
    when(delegate.poll(any(Duration.class))).thenReturn(new ConsumerRecords<>(
        Map.of(
            new TopicPartition("foo", 0), List.of(fooRecord0),
            new TopicPartition("foo", 1), List.of(fooRecord1),
            new TopicPartition("bar", 1), List.of(barRecord)
        )
    ));

    // When:
    final ConsumerRecords<byte[], byte[]> result = consumer.poll(Duration.ZERO);
    final List<ConsumerRecord<byte[], byte[]>> recs = result.records(new TopicPartition("foo", 0));

    // Then:
    assertThat(recs, Matchers.containsInAnyOrder(fooRecord0, fooRecord1));
  }

  @Test
  public void shouldDelegatePositionCallsForPartitionInAssignment() {
    // Given:
    final TopicPartition partition = new TopicPartition("foo", 0);
    when(delegate.assignment()).thenReturn(Set.of(partition));
    when(delegate.position(eq(partition), any(Duration.class))).thenReturn(101L);

    // When:
    final long position = consumer.position(partition, Duration.ZERO);

    // Then:
    assertThat(position, is(101L));
    verifyNoInteractions(admin);
  }

  @Test
  public void shouldUseAdminForPositionCallsForPartitionNotInAssignment() {
    // Given:
    final TopicPartition assignedPartition = new TopicPartition("foo", 0);
    final TopicPartition otherPartition = new TopicPartition("foo", 1);
    final ListConsumerGroupOffsetsResult res = Mockito.mock(ListConsumerGroupOffsetsResult.class);
    when(delegate.groupMetadata()).thenReturn(new ConsumerGroupMetadata("group.id"));
    when(delegate.assignment()).thenReturn(Set.of(assignedPartition));
    when(res.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(
            Map.of(otherPartition, new OffsetAndMetadata(101L, "meta"))));
    when(admin.listConsumerGroupOffsets(any(String.class)))
        .thenReturn(res);

    // When:
    final long position = consumer.position(otherPartition, Duration.ZERO);

    // Then:
    assertThat(position, is(101L));
    verify(delegate, never()).position(otherPartition);
  }
}