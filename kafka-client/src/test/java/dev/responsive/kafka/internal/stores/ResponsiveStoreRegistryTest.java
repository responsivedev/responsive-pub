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
      o -> {}
  );

  private static final ResponsiveStoreRegistration UNINIT_REGISTRATION =
      new ResponsiveStoreRegistration(
          "store",
          UNINIT_TOPIC_PARTITION,
          OptionalLong.empty(),
          o -> { }
      );

  private final ResponsiveStoreRegistry registry = new ResponsiveStoreRegistry();

  @BeforeEach
  public void setup() {
    registry.registerStore(REGISTRATION);
    registry.registerStore(UNINIT_REGISTRATION);
  }

  @Test
  public void shouldGetCommittedOffsetFromRegisteredStore() {
    assertThat(registry.getCommittedOffset(TOPIC_PARTITION), is(OptionalLong.of(123L)));
  }

  @Test
  public void shouldReturnEmptyCommittedOffsetFromNotRegisteredStore() {
    assertThat(
        registry.getCommittedOffset(new TopicPartition("foo", 1)),
        is(OptionalLong.empty())
    );
  }

  @Test
  public void shouldReturnEmptyCommittedOffsetFromChangelogWithNoOffset() {
    assertThat(
        registry.getCommittedOffset(UNINIT_TOPIC_PARTITION),
        is(OptionalLong.empty())
    );
  }

  @Test
  public void shouldDeregisterStore() {
    // given:
    registry.deregisterStore(REGISTRATION);

    // when:
    final OptionalLong offset = registry.getCommittedOffset(TOPIC_PARTITION);

    // then:
    assertThat(offset, is(OptionalLong.empty()));
  }
}