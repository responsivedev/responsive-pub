package dev.responsive.examples.e2etest;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.responsive.examples.common.JsonDeserializer;
import dev.responsive.examples.common.JsonSerde;
import dev.responsive.examples.common.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;

public class E2ESchema {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    OBJECT_MAPPER.registerModules();
  }

  record InputRecord(long value, long count) {}

  record OutputRecord(long value, long count, long offset, byte[] digest) {}

  static Serde<InputRecord> inputRecordSerde() {
    return new JsonSerde<>(InputRecord.class);
  }

  static Serde<OutputRecord> outputRecordSerde() {
    return new JsonSerde<>(OutputRecord.class);
  }

  public static class InputRecordSerializer extends JsonSerializer<InputRecord> {
    public InputRecordSerializer() {
      super(InputRecord.class);
    }
  }

  public static class OutputRecordDeserializer extends JsonDeserializer<OutputRecord> {
    public OutputRecordDeserializer() {
      super(OutputRecord.class);
    }
  }

  private E2ESchema() {
  }
}
