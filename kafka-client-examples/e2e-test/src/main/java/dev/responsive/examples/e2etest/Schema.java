package dev.responsive.examples.e2etest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class Schema {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    OBJECT_MAPPER.registerModules();
  }

  record InputRecord(long value, long count) {}

  record OutputRecord(long value, long count, long offset, byte[] digest) {}

  static Serde<InputRecord> inputRecordSerde() {
    return new JsonSerdes<>(new InputRecordSerializer(), new InputRecordDeserializer());
  }

  static Serde<OutputRecord> outputRecordSerde() {
    return new JsonSerdes<>(new OutputRecordSerializer(), new OutputRecordDeserializer());
  }

  public static class InputRecordSerializer extends JsonSerializer<InputRecord> {
    public InputRecordSerializer() {
    }
  }

  public static class OutputRecordSerializer extends JsonSerializer<OutputRecord> {
    public OutputRecordSerializer() {
    }
  }

  public static class InputRecordDeserializer extends JsonDeserializer<InputRecord> {
    public InputRecordDeserializer() {
      super(InputRecord.class);
    }
  }

  public static class OutputRecordDeserializer extends JsonDeserializer<OutputRecord> {
    public OutputRecordDeserializer() {
      super(OutputRecord.class);
    }
  }

  private static class JsonSerializer<T> implements Serializer<T> {
    @Override
    public byte[] serialize(final String topic, final T data) {
      try {
        return OBJECT_MAPPER.writeValueAsBytes(data);
      } catch (final JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class JsonDeserializer<T> implements Deserializer<T> {
    private final Class<T> clazz;

    private JsonDeserializer(final Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public T deserialize(final String topic, final byte[] data) {
      try {
        return OBJECT_MAPPER.readValue(data, clazz);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class JsonSerdes<T> implements Serde<T> {
    private final JsonDeserializer<T> deserializer;
    private final JsonSerializer<T> serializer;

    private JsonSerdes(final JsonSerializer<T> serializer, final JsonDeserializer<T> deserializer) {
      this.deserializer = deserializer;
      this.serializer = serializer;
    }

    @Override
    public Serializer<T> serializer() {
      return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
      return deserializer;
    }
  }

  private Schema() {
  }
}
