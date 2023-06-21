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

package dev.responsive.kafka.clients;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import dev.responsive.kafka.clients.ResponsiveProducer.Listener;
import dev.responsive.kafka.clients.ResponsiveProducer.RecordingKey;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ResponsiveProducerTest {
  private static final TopicPartition PARTITION1 = new TopicPartition("polar", 1);
  private static final TopicPartition PARTITION2 = new TopicPartition("panda", 2);

  @Mock
  private Producer<?, ?> wrapped;
  @Mock
  private Listener listener1;
  @Mock
  private Listener listener2;
  private ResponsiveProducer<?, ?> producer;

  @BeforeEach
  public void setup() {
    producer = new ResponsiveProducer<>("clientid", wrapped, List.of(listener1, listener2));
  }

  @Test
  public void shouldNotifyOnCommit() {
    // given:
    producer.sendOffsetsToTransaction(
        Map.of(
            PARTITION1, new OffsetAndMetadata(10),
            PARTITION2, new OffsetAndMetadata(11)
        ),
        "foo"
    );

    // when:
    producer.commitTransaction();

    // then:
    final var expected = Map.of(
        new RecordingKey(PARTITION1, "foo"), 10L,
        new RecordingKey(PARTITION2, "foo"), 11L
    );
    verify(listener1).onCommit(expected);
    verify(listener2).onCommit(expected);
  }

  @Test
  public void shouldClearOffsetsOnAbort() {
    // given:
    producer.sendOffsetsToTransaction(Map.of(PARTITION1, new OffsetAndMetadata(10)), "foo");

    // when:
    producer.abortTransaction();

    // then:
    producer.sendOffsetsToTransaction(Map.of(PARTITION2, new OffsetAndMetadata(11)), "foo");
    producer.commitTransaction();
    final var expected = Map.of(new RecordingKey(PARTITION2, "foo"), 11L);
    verify(listener1).onCommit(expected);
    verify(listener2).onCommit(expected);
  }

  @Test
  public void shouldClearOffsetsOnCommit() {
    // given:
    producer.sendOffsetsToTransaction(Map.of(PARTITION1, new OffsetAndMetadata(10)), "foo");

    // when:
    producer.commitTransaction();

    // then:
    producer.sendOffsetsToTransaction(Map.of(PARTITION2, new OffsetAndMetadata(11)), "foo");
    producer.commitTransaction();
    final var expected1 = Map.of(new RecordingKey(PARTITION1, "foo"), 10L);
    final var expected2 = Map.of(new RecordingKey(PARTITION2, "foo"), 11L);
    verify(listener1).onCommit(expected1);
    verify(listener2).onCommit(expected1);
    verify(listener1).onCommit(expected2);
    verify(listener2).onCommit(expected2);
  }

  @Test
  public void shouldIgnoreExceptionFromCommitCallback() {
    // given:
    producer.sendOffsetsToTransaction(Map.of(PARTITION1, new OffsetAndMetadata(10)), "foo");
    doThrow(new RuntimeException("oops")).when(listener1).onCommit(any());

    // when:
    producer.commitTransaction();

    // then:
    final var expected = Map.of(new RecordingKey(PARTITION1, "foo"), 10L);
    verify(listener1).onCommit(expected);
    verify(listener2).onCommit(expected);
  }

  @Test
  public void shouldNotifyOnClose() {
    // when:
    producer.close();

    // then:
    verify(listener1).onClose();
    verify(listener2).onClose();
  }

  @Test
  public void shouldIgnoreExceptionFromCloseCallback() {
    // given:
    doThrow(new RuntimeException("oops")).when(listener1).onClose();

    // when:
    producer.close();

    // then:
    verify(listener1).onClose();
    verify(listener2).onClose();
  }
}