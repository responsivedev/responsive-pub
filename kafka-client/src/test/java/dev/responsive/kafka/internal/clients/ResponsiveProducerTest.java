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

package dev.responsive.kafka.internal.clients;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.responsive.kafka.internal.clients.ResponsiveProducer;
import dev.responsive.kafka.internal.clients.ResponsiveProducer.Listener;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ResponsiveProducerTest {
  private static final TopicPartition PARTITION1 = new TopicPartition("polar", 1);
  private static final TopicPartition PARTITION2 = new TopicPartition("panda", 2);

  @Mock
  private Producer<String, String> wrapped;
  @Mock
  private Listener listener1;
  @Mock
  private Listener listener2;
  @Captor
  private ArgumentCaptor<Callback> callbackCaptor;
  private ResponsiveProducer<String, String> producer;

  @BeforeEach
  public void setup() {
    producer = new ResponsiveProducer<>("clientid", wrapped, List.of(listener1, listener2));
  }

  @Test
  public void shouldNotifyOnCommit() {
    // when:
    producer.commitTransaction();

    // then:
    verify(listener1).onCommit();
    verify(listener2).onCommit();
  }

  @Test
  public void shouldNotifyOnSendCallback() {
    // given:
    final var record = new ProducerRecord<String, String>(PARTITION1.topic(), "val");
    final var recordRef = new AtomicReference<RecordMetadata>();
    final var exceptionRef = new AtomicReference<Exception>();
    producer.send(record, (rm, e) -> {
      recordRef.set(rm);
      exceptionRef.set(e);
    });
    verify(wrapped).send(same(record), callbackCaptor.capture());
    final RecordMetadata recordMetadata = new RecordMetadata(
        PARTITION1,
        123L,
        0,
        0,
        0,
        0
    );

    // when:
    callbackCaptor.getValue().onCompletion(recordMetadata, null);

    // then:
    assertThat(recordRef.get(), is(recordMetadata));
    assertThat(exceptionRef.get(), is(nullValue()));
    verify(listener1).onSendCompleted(recordMetadata);
    verify(listener2).onSendCompleted(recordMetadata);
  }

  @SuppressWarnings("unchecked")
  private Future<RecordMetadata> mockFuture() {
    return mock(Future.class);
  }

  @Test
  public void shouldNotifyOnSendFutureAndReturnRecordMetadata()
      throws InterruptedException, ExecutionException {
    // given:
    final var future = mockFuture();
    when(wrapped.send(any())).thenReturn(future);
    final RecordMetadata recordMetadata = new RecordMetadata(
        PARTITION1,
        123L,
        0,
        0,
        0,
        0
    );
    when(future.get()).thenReturn(recordMetadata);
    final var returnedFuture = producer.send(new ProducerRecord<>(PARTITION1.topic(), "val"));

    // when:
    final var returnedMetadata = returnedFuture.get();

    // then:
    assertThat(returnedMetadata, is(recordMetadata));
    verify(listener1).onSendCompleted(recordMetadata);
    verify(listener2).onSendCompleted(recordMetadata);
  }

  @Test
  public void shouldNotifyOnSendOffsetsToTransaction() {
    // when:
    producer.sendOffsetsToTransaction(
        Map.of(
            PARTITION1, new OffsetAndMetadata(10),
            PARTITION2, new OffsetAndMetadata(11)
        ),
        "foo"
    );

    // then:
    final var expected = Map.of(
        PARTITION1, new OffsetAndMetadata(10L),
        PARTITION2, new OffsetAndMetadata(11L)
    );
    verify(listener1).onSendOffsetsToTransaction(expected, "foo");
    verify(listener2).onSendOffsetsToTransaction(expected, "foo");
  }

  @Test
  public void shouldThrowExceptionFromCommitCallback() {
    // given:
    producer.sendOffsetsToTransaction(Map.of(PARTITION1, new OffsetAndMetadata(10)), "foo");
    doThrow(new RuntimeException("oops")).when(listener1).onCommit();

    // when/then:
    assertThrows(RuntimeException.class, () -> producer.commitTransaction());
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