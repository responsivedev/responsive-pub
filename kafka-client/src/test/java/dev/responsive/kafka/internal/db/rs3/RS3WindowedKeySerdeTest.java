package dev.responsive.kafka.internal.db.rs3;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import dev.responsive.kafka.internal.utils.WindowedKey;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

class RS3WindowedKeySerdeTest {

  @Property
  public void shouldSerializeAndDeserialize(
      @ForAll byte[] key,
      @ForAll long timestamp
  ) {
    final var windowKey = new WindowedKey(key, timestamp);
    final var serde = new RS3WindowedKeySerde();
    final var serialized = serde.serialize(windowKey);
    assertThat(serde.deserialize(serialized), is(windowKey));
  }

}