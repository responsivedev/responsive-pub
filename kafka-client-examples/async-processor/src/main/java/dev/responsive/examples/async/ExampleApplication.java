package dev.responsive.examples.async;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.responsive.kafka.api.async.AsyncFixedKeyProcessorSupplier;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.StoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;

public class ExampleApplication {
  private static final Logger LOG = LoggerFactory.getLogger(ExampleApplication.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String WEBHOOK = "<WEBHOOK>";
  private static final String OPENAI_TOKEN = "<OPENAI_TOKEN>";

  public static Topology buildTopology() {
    final var builder = new StreamsBuilder();
    final KStream<String, IncomingChatRequest> stream = builder.stream(
        "chat-async-example",
        Consumed.with(
            Serdes.String(),
            new JsonSerde<>(IncomingChatRequest.class)
        )
    );

    // enrich the incoming messages with data from DynamoDB
    final KStream<String, EnrichedChatRequest> withUserMeta = stream.processValues(
        AsyncFixedKeyProcessorSupplier.createAsyncProcessorSupplier(
            new EnrichProcessorSupplier()
        ),
        Named.as("ENRICH")
    );

    // scan each msg against OpenAI's moderation model
    final KStream<String, ScannedChatRequest> scanned = withUserMeta.processValues(
        AsyncFixedKeyProcessorSupplier.createAsyncProcessorSupplier(
            new ScanProcessorSupplier(OPENAI_TOKEN)
        ),
        Named.as("MODERATE")
    );
    final var branched = scanned.split();
    // send messages that were not flagged to the output topic
    branched.branch(
        (k, v) -> !v.flagged,
        Branched.withConsumer(ks -> ks.to(
            "chat-async-example-routed",
            Produced.with(
                Serdes.String(),
                new JsonSerde<>(ScannedChatRequest.class)
            )
        ))
    );
    // send flagged messages to a slack channel
    branched.branch(
        (k, v) -> v.flagged,
        Branched.withConsumer(ks -> ks.processValues(
            AsyncFixedKeyProcessorSupplier.createAsyncProcessorSupplier(
                new ReportProcessorSupplier(WEBHOOK)
            ),
            Named.as("NOTIFY")))
    );

    LOG.info("built topology");

    return builder.build();
  }

  public record IncomingChatRequest(String username, String msg) {}

  public record EnrichedChatRequest(
      String username,
      String geo,
      String msg
  ) {}

  public record ScannedChatRequest(EnrichedChatRequest original, boolean flagged) {}

  public static class JsonSerde<T> implements Serde<T> {
    private final Class<T> clazz;

    public JsonSerde(final Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public Serializer<T> serializer() {
      return new JsonSerializer<>();
    }

    @Override
    public Deserializer<T> deserializer() {
      return new JsonDeserializer<>(clazz);
    }
  }

  public static class JsonSerializer<T> implements Serializer<T> {
    @Override
    public byte[] serialize(String s, T v) {
      try {
        return OBJECT_MAPPER.writeValueAsBytes(v);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class JsonDeserializer<T> implements Deserializer<T> {
    private final Class<T> clazz;

    public JsonDeserializer(final Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
      try {
        return OBJECT_MAPPER.readValue(bytes, clazz);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static final class EnrichProcessorSupplier
      implements FixedKeyProcessorSupplier<String, IncomingChatRequest, EnrichedChatRequest> {
    @Override
    public FixedKeyProcessor<String, IncomingChatRequest, EnrichedChatRequest> get() {
      return new EnrichProcessor();
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
      return FixedKeyProcessorSupplier.super.stores();
    }
  }

  public static final class EnrichProcessor
      implements FixedKeyProcessor<String, IncomingChatRequest, EnrichedChatRequest> {
    DynamoDbClient ddb;
    FixedKeyProcessorContext<String, EnrichedChatRequest> context;

    @Override
    public void init(final FixedKeyProcessorContext<String, EnrichedChatRequest> context) {
      ddb = DynamoDbClient.builder()
          .region(Region.US_WEST_2)
          .build();
      this.context = context;
    }

    @Override
    public void process(final FixedKeyRecord<String, IncomingChatRequest> fixedKeyRecord) {
      final var value = fixedKeyRecord.value();
      final Map<String, AttributeValue> keyToGet = Map.of(
          "username", AttributeValue.builder().s(value.username).build()
      );
      final GetItemRequest request = GetItemRequest.builder()
          .key(keyToGet)
          .tableName("async-chat-example")
          .build();
      final Map<String, AttributeValue> returnedItem = ddb.getItem(request).item();
      if (!returnedItem.isEmpty()) {
        final String geo = returnedItem.get("geo").s();
        context.forward(fixedKeyRecord.withValue(
            new EnrichedChatRequest(value.username, geo, value.msg)
        ));
      } else {
        LOG.info("no user found with username {}", value.username);
      }
    }
  }

  public static final class ScanProcessorSupplier
      implements FixedKeyProcessorSupplier<String, EnrichedChatRequest, ScannedChatRequest> {
    private final String token;

    public ScanProcessorSupplier(final String token) {
      this.token = token;
    }

    @Override
    public FixedKeyProcessor<String, EnrichedChatRequest, ScannedChatRequest> get() {
      return new ScanProcessor(token);
    }
  }

  public static final class ScanProcessor
      implements FixedKeyProcessor<String, EnrichedChatRequest, ScannedChatRequest> {
    final String token;
    HttpClient httpClient;
    FixedKeyProcessorContext<String, ScannedChatRequest> context;

    public ScanProcessor(final String token) {
      this.token = token;
    }

    @Override
    public void init(final FixedKeyProcessorContext<String, ScannedChatRequest> context) {
      httpClient = HttpClient.newHttpClient();
      this.context = context;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(final FixedKeyRecord<String, EnrichedChatRequest> fixedKeyRecord) {
      final var value = fixedKeyRecord.value();
      final Map<String, String> bodyMap = Map.of(
          "input", value.msg
      );
      final boolean flagged;
      try {
        final String body = OBJECT_MAPPER.writeValueAsString(bodyMap);
        final var request = HttpRequest.newBuilder(URI.create("https://api.openai.com/v1/moderations"))
          .POST(HttpRequest.BodyPublishers.ofString(body))
          .header("Authorization", "Bearer " + token)
          .build();
        final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        final Map<String, Object> responseMap = OBJECT_MAPPER.readValue(response.body(), Map.class);
        flagged = (boolean)
            ((Map<String, Object>)((List<Object>) responseMap.get("results")).get(0)).get("flagged");
      } catch (final IOException|InterruptedException e) {
        throw new RuntimeException(e);
      }
      if (flagged) {
        LOG.info("flagging msg from user: {} {}", value.username, value.msg);
      }
      context.forward(
          fixedKeyRecord.withValue(new ScannedChatRequest(value, flagged))
      );
    }
  }

  public static final class ReportProcessorSupplier
      implements FixedKeyProcessorSupplier<String, ScannedChatRequest, ScannedChatRequest> {
    private final String webhook;

    public ReportProcessorSupplier(final String webhook) {
      this.webhook = webhook;
    }

    @Override
    public FixedKeyProcessor<String, ScannedChatRequest, ScannedChatRequest> get() {
      return new ReportProcessor(webhook);
    }
  }

  public static final class ReportProcessor
      implements FixedKeyProcessor<String, ScannedChatRequest, ScannedChatRequest> {
    private final String webhook;
    HttpClient httpClient;

    public ReportProcessor(final String webhook) {
      this.webhook = webhook;
    }

    @Override
    public void init(final FixedKeyProcessorContext<String, ScannedChatRequest> context) {
      httpClient = HttpClient.newHttpClient();
    }

    @Override
    public void process(final FixedKeyRecord<String, ScannedChatRequest> fixedKeyRecord) {
      final Map<String, String> bodyMap = Map.of(
          "text", "message flagged: " + fixedKeyRecord.key()
      );
      try {
        final String body = OBJECT_MAPPER.writeValueAsString(bodyMap);
        final var request = HttpRequest.newBuilder(URI.create(webhook))
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
        httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      } catch (final IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
