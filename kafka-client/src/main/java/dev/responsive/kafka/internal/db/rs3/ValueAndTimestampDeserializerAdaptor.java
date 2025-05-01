package dev.responsive.kafka.internal.db.rs3;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;

public class ValueAndTimestampDeserializerAdaptor<V> implements Deserializer<V> {
  private final Deserializer<ValueAndTimestamp<V>> valueAndTimestampDeserializer;

  public ValueAndTimestampDeserializerAdaptor(
      final Serde<V> valueSerde
  ) {
    this.valueAndTimestampDeserializer = new ValueAndTimestampSerde<>(valueSerde).deserializer();
  }

  @Override
  public V deserialize(String topic, byte[] data) {
    return valueAndTimestampDeserializer.deserialize(topic, data).value();
  }
}
