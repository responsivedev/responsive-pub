package dev.responsive.kafka.store;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.OptionalLong;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ResponsiveStoreRegistryTest {
  private static final TopicPartition TOPIC_PARTITION = new TopicPartition("changelog-topic", 5);
  private static final ResponsiveStoreRegistration REGISTRATION =
      new ResponsiveStoreRegistration("store", TOPIC_PARTITION, 123L, o -> {});

  private final ResponsiveStoreRegistry registry = new ResponsiveStoreRegistry();

  @BeforeEach
  public void setup() {
    registry.registerStore(REGISTRATION);
  }

  @Test
  public void shouldGetCommittedOffsetFromRegisteredStore() {
    assertThat(registry.getCommittedOffset(TOPIC_PARTITION), is(OptionalLong.of(123L)));
  }

  @Test
  public void shouldReturnEmptyCommittedOffsetFromNotRegisteredStore() {
    assertThat(registry.getCommittedOffset(new TopicPartition("foo", 1)), is(OptionalLong.empty()));
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
