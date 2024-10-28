package dev.responsive.kafka.testutils;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;

public class SerdeUtils {

  private static final Serde<String> STRING_SERDE = Serdes.String();
  private static final Serde<ValueAndTimestamp<String>> VALUE_AND_TIMESTAMP_STRING_SERDE =
      new ValueAndTimestampSerde<>(STRING_SERDE);

  public static <D> byte[] serialize(final D data, final Serde<D> serde) {
    return serde.serializer().serialize("ignored", data);
  }

  public static Bytes serializedKey(final String key) {
    return Bytes.wrap(serialize(key, STRING_SERDE));
  }

  public static byte[] serializedValue(final String value) {
    return serialize(value, STRING_SERDE);
  }

  public static byte[] serializedValueAndTimestamp(final String value, final long timestamp) {
    final ValueAndTimestamp<String> valueAndTimestamp = ValueAndTimestamp.make(value, timestamp);
    return serialize(valueAndTimestamp, VALUE_AND_TIMESTAMP_STRING_SERDE);
  }

}
