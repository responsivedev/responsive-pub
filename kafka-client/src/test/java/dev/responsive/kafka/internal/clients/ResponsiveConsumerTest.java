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

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import dev.responsive.kafka.internal.utils.GroupTraceRoot;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ResponsiveConsumerTest {
  private static final TopicPartition PARTITION = new TopicPartition("baguette", 1);
  @Mock
  private Consumer<?, ?> wrapped;
  @Mock
  private ConsumerRebalanceListener providedRebalanceListener;
  @Mock
  private ResponsiveConsumer.Listener listener1;
  @Mock
  private ResponsiveConsumer.Listener listener2;
  @Captor
  private ArgumentCaptor<ConsumerRebalanceListener> rebalanceListenerCaptor;

  private ResponsiveConsumer<?, ?> consumer;

  @BeforeEach
  public void setup() {
    consumer = new ResponsiveConsumer<>(
        "clientid", wrapped, List.of(listener1, listener2), GroupTraceRoot.create("foo"));
  }

  @Test
  public void shouldNotifyOnPartitionsAssigned() {
    // given:
    consumer.subscribe(List.of("baguette", "pita"), providedRebalanceListener);
    final var rebalanceListener = verifyAndGetCapturedRebalanceListener();

    // when:
    rebalanceListener.onPartitionsAssigned(List.of(PARTITION));

    // then:
    verify(providedRebalanceListener).onPartitionsAssigned(List.of(PARTITION));
    verify(listener1).onPartitionsAssigned(List.of(PARTITION));
    verify(listener2).onPartitionsAssigned(List.of(PARTITION));
  }

  @Test
  public void shouldThrowWhenNoListenerProvided() {
    assertThrows(
        IllegalStateException.class,
        () -> consumer.subscribe(List.of("baguette", "pita"))
    );
  }

  @Test
  public void shouldNotifyOnPartitionsAssignedWithPatternSubscribe() {
    // given:
    consumer.subscribe(Pattern.compile(".*baguette.*"), providedRebalanceListener);
    final var rebalanceListener = verifyAndGetCapturedRebalanceListenerForPatternSubscribe();

    // when:
    rebalanceListener.onPartitionsAssigned(List.of(PARTITION));

    // then:
    verify(listener1).onPartitionsAssigned(List.of(PARTITION));
    verify(listener2).onPartitionsAssigned(List.of(PARTITION));
  }

  @Test
  public void shouldIgnoreErrorsOnAssignCallback() {
    // given:
    consumer.subscribe(List.of("baguette"), providedRebalanceListener);
    final var rebalanceListener = verifyAndGetCapturedRebalanceListener();
    doThrow(new RuntimeException("oops")).when(listener1).onPartitionsAssigned(any());

    // when:
    rebalanceListener.onPartitionsAssigned(List.of(PARTITION));

    // then:
    verify(listener2).onPartitionsAssigned(List.of(PARTITION));
  }

  @Test
  public void shouldNotifyOnPartitionsRevoked() {
    // given:
    consumer.subscribe(List.of("baguette"), providedRebalanceListener);
    final var rebalanceListener = verifyAndGetCapturedRebalanceListener();

    // when:
    rebalanceListener.onPartitionsRevoked(List.of(PARTITION));

    // then:
    verify(providedRebalanceListener).onPartitionsRevoked(List.of(PARTITION));
    verify(listener1).onPartitionsRevoked(List.of(PARTITION));
    verify(listener2).onPartitionsRevoked(List.of(PARTITION));
  }

  @Test
  public void shouldNotifyOnPartitionsLost() {
    // given:
    consumer.subscribe(List.of("baguette"), providedRebalanceListener);
    final var rebalanceListener = verifyAndGetCapturedRebalanceListener();

    // when:
    rebalanceListener.onPartitionsLost(List.of(PARTITION));

    // then:
    verify(providedRebalanceListener).onPartitionsLost(List.of(PARTITION));
    verify(listener1).onPartitionsLost(List.of(PARTITION));
    verify(listener2).onPartitionsLost(List.of(PARTITION));
  }

  @Test
  public void shouldIgnoreErrorsOnRevokedCallback() {
    // given:
    consumer.subscribe(List.of("baguette"), providedRebalanceListener);
    final var rebalanceListener = verifyAndGetCapturedRebalanceListener();
    doThrow(new RuntimeException("oops")).when(listener1).onPartitionsRevoked(any());

    // when:
    rebalanceListener.onPartitionsRevoked(List.of(PARTITION));

    // then:
    verify(listener2).onPartitionsRevoked(List.of(PARTITION));
  }

  @Test
  public void shouldIgnoreErrorsOnLostCallback() {
    // given:
    consumer.subscribe(List.of("baguette"), providedRebalanceListener);
    final var rebalanceListener = verifyAndGetCapturedRebalanceListener();
    doThrow(new RuntimeException("oops")).when(listener1).onPartitionsLost(any());

    // when:
    rebalanceListener.onPartitionsLost(List.of(PARTITION));

    // then:
    verify(listener2).onPartitionsLost(List.of(PARTITION));
  }

  @Test
  public void shouldNotifyOnClose() {
    // when:
    consumer.close();

    // then:
    verify(listener1).onClose();
    verify(listener2).onClose();
  }

  @Test
  public void shouldNotifyOnCommitSync() {
    // given:
    final var commits = Map.of(PARTITION, new OffsetAndMetadata(123L));

    // when:
    consumer.commitSync(commits);

    // then:
    verify(listener1).onCommit(commits);
    verify(listener2).onCommit(commits);
  }

  @Test
  public void shouldNotifyOnCommitSyncWithTimeout() {
    // given:
    final var commits = Map.of(PARTITION, new OffsetAndMetadata(123L));

    // when:
    consumer.commitSync(commits, Duration.ofSeconds(30));

    // then:
    verify(listener1).onCommit(commits);
    verify(listener2).onCommit(commits);
  }

  @Test
  public void shouldThrowOnCommitsWithoutOffsetsAndAsyncCommits() {
    assertThrows(UnsupportedOperationException.class, () -> consumer.commitSync());
    assertThrows(UnsupportedOperationException.class, () -> consumer.commitAsync());
    assertThrows(UnsupportedOperationException.class, () -> consumer.commitAsync((c, e) -> {}));
    assertThrows(
        UnsupportedOperationException.class, () -> consumer.commitAsync(Map.of(), (c, e) -> {})
    );
  }

  @Test
  public void shouldIgnoreErrorsOnCloseCallback() {
    // given:
    doThrow(new RuntimeException("oops")).when(listener1).onClose();

    // when:
    consumer.close();

    // then:
    verify(listener1).onClose();
    verify(listener2).onClose();
  }

  @SuppressWarnings("unchecked")
  private ConsumerRebalanceListener verifyAndGetCapturedRebalanceListener() {
    verify(wrapped).subscribe(any(Collection.class), rebalanceListenerCaptor.capture());
    return rebalanceListenerCaptor.getValue();
  }

  private ConsumerRebalanceListener verifyAndGetCapturedRebalanceListenerForPatternSubscribe() {
    verify(wrapped).subscribe(any(Pattern.class), rebalanceListenerCaptor.capture());
    return rebalanceListenerCaptor.getValue();
  }
}