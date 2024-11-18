/*
 * Copyright 2024 Responsive Computing, Inc.
 *
 * This source code is licensed under the Responsive Business Source License Agreement v1.0
 * available at:
 *
 * https://www.responsive.dev/legal/responsive-bsl-10
 *
 * This software requires a valid Commercial License Key for production use. Trial and commercial
 * licenses can be obtained at https://www.responsive.dev
 */

package dev.responsive.kafka.internal.stores;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistry;
import java.util.OptionalLong;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ResponsiveStoreRegistryTest {
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition("changelog-topic", 5);
  private static final TopicPartition UNINIT_TOPIC_PARTITION =
      new TopicPartition("changelog-topic", 2);
  private static final ResponsiveStoreRegistration REGISTRATION = new ResponsiveStoreRegistration(
      "store",
      TOPIC_PARTITION,
      OptionalLong.of(123L),
      o -> {},
      "thread"
  );

  private static final ResponsiveStoreRegistration UNINIT_REGISTRATION =
      new ResponsiveStoreRegistration(
          "store",
          UNINIT_TOPIC_PARTITION,
          OptionalLong.empty(),
          o -> { },
          "thread"
      );

  private final ResponsiveStoreRegistry registry = new ResponsiveStoreRegistry();

  @BeforeEach
  public void setup() {
    registry.registerStore(REGISTRATION);
    registry.registerStore(UNINIT_REGISTRATION);
  }

  @Test
  public void shouldGetCommittedOffsetFromRegisteredStore() {
    assertThat(registry.getCommittedOffset(TOPIC_PARTITION, "thread"),
        is(OptionalLong.of(123L)));
  }

  @Test
  public void shouldReturnEmptyCommittedOffsetFromNotRegisteredStore() {
    assertThat(
        registry.getCommittedOffset(new TopicPartition("foo", 1), "thread"),
        is(OptionalLong.empty())
    );
  }

  @Test
  public void shouldReturnEmptyCommittedOffsetFromChangelogWithNoOffset() {
    assertThat(
        registry.getCommittedOffset(UNINIT_TOPIC_PARTITION, "thread"),
        is(OptionalLong.empty())
    );
  }

  @Test
  public void shouldDeregisterStore() {
    // given:
    registry.deregisterStore(REGISTRATION);

    // when:
    final OptionalLong offset = registry.getCommittedOffset(TOPIC_PARTITION, "thread");

    // then:
    assertThat(offset, is(OptionalLong.empty()));
  }
}