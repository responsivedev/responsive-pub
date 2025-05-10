package dev.responsive.kafka.internal.snapshot.topic;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.TaskId;

public class SnapshotStoreSerdes {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerModule(new JavaTimeModule());
    MAPPER.registerModule(new Jdk8Module());
    final SimpleModule module = new SimpleModule();
    module.addSerializer(TaskId.class, new TaskIDJacksonSerializer());
    module.addDeserializer(TaskId.class, new TaskIDJacksonDeserializer());
    MAPPER.registerModule(module);
    MAPPER.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
  }

  public static class SnapshotStoreRecordKeySerializer
      implements Serializer<SnapshotStoreRecordKey> {
    @Override
    public byte[] serialize(String topic, SnapshotStoreRecordKey data) {
      try {
        return MAPPER.writeValueAsBytes(data);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class SnapshotStoreRecordKeyDeserializer
      implements Deserializer<SnapshotStoreRecordKey> {
    @Override
    public SnapshotStoreRecordKey deserialize(String topic, byte[] data) {
      try {
        return MAPPER.readValue(data, SnapshotStoreRecordKey.class);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class SnapshotStoreRecordSerializer implements Serializer<SnapshotStoreRecord> {
    @Override
    public byte[] serialize(String topic, SnapshotStoreRecord data) {
      try {
        return MAPPER.writeValueAsBytes(data);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class SnapshotStoreRecordDeserializer implements Deserializer<SnapshotStoreRecord> {
    @Override
    public SnapshotStoreRecord deserialize(String topic, byte[] data) {
      try {
        return MAPPER.readValue(data, SnapshotStoreRecord.class);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class TaskIDJacksonSerializer extends JsonSerializer<TaskId> {
    @Override
    public void serialize(
        final TaskId taskId,
        final JsonGenerator jsonGenerator,
        final SerializerProvider serializerProvider
    ) throws IOException {
      jsonGenerator.writeString(taskId.toString());
    }
  }

  public static class TaskIDJacksonDeserializer extends JsonDeserializer<TaskId> {

    @Override
    public TaskId deserialize(
        final JsonParser jsonParser,
        final DeserializationContext deserializationContext
    ) throws IOException {
      return TaskId.parse(jsonParser.getValueAsString());
    }
  }
}
