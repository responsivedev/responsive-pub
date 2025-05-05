package dev.responsive.kafka.internal.snapshot;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.responsive.kafka.internal.snapshot.topic.SnapshotStoreRecord;
import dev.responsive.kafka.internal.snapshot.topic.SnapshotStoreRecordKey;
import dev.responsive.kafka.internal.snapshot.topic.SnapshotStoreSerdes;
import dev.responsive.kafka.internal.snapshot.topic.TopicSnapshotStore;
import dev.responsive.kafka.testutils.TestConstants;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.lifecycle.Startables;

@ExtendWith(MockitoExtension.class)
class SnapshotOrchestrationIntegrationTest {
  private static KafkaContainer KAFKA = new KafkaContainer(TestConstants.KAFKA)
      .withEnv("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "1000")
      .withEnv("KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS", "60000");
  private static final Set<TaskId> TASKS = Set.of(
      new TaskId(0, 0),
      new TaskId(0, 1),
      new TaskId(0, 2)
  );

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      KAFKA.stop();
    }));
  }

  private String topic;
  private TestCtx ctx;

  @BeforeAll
  public static void setupAll() {
    final var fut = Startables.deepStart(KAFKA);
    try {
      fut.get();
    } catch (final InterruptedException | ExecutionException e) {
      System.out.println("LOGS: " + KAFKA.getLogs());
      throw new RuntimeException(e);
    }
  }

  @AfterAll
  public static void teardownAll() {
    KAFKA.stop();
  }

  @BeforeEach
  public void setup(final TestInfo testInfo) {
    topic = testInfo.getTestMethod().get().getName();
    ctx = createCtx();
  }

  @Test
  public void shouldReturnGenerationZeroSnapshot() {
    // when:
    final var snapshot = ctx.store.currentSnapshot(true);

    // then:
    assertThat(snapshot, is(Snapshot.initial()));
  }

  @Test
  public void shouldCreateInitialSnapshot() {
    // when:
    final var snapshot = ctx.api.createSnapshot();

    // then:
    MatcherAssert.assertThat(snapshot, Matchers.is(ctx.store.currentSnapshot(true)));
    assertThat(snapshot.generation(), is(1L));
    assertThat(snapshot.state(), is(Snapshot.State.CREATED));
  }

  @Test
  public void shouldRefreshStore() {
    // given:
    final var store2 = createCtx();
    store2.api.createSnapshot();

    // when:
    final var snapshot = ctx.store.currentSnapshot(true);

    // then:
    MatcherAssert.assertThat(snapshot, Matchers.is(ctx.store.currentSnapshot(true)));
    MatcherAssert.assertThat(snapshot.generation(), is(1L));
    MatcherAssert.assertThat(snapshot.state(), is(Snapshot.State.CREATED));
  }

  @Test
  public void shouldFailCreateNextSnapshotIfNotCompleted() {
    // given:
    ctx.api.createSnapshot();

    // when/then:
    assertThrows(RuntimeException.class, () -> ctx.api.createSnapshot());
  }

  @Test
  public void shouldCreatNextSnapshot() {
    // given:
    ctx.api.createSnapshot();
    ctx.orchestrator.reportTaskSnapshotMetadata(1, List.of(
        new Snapshot.TaskSnapshotMetadata(
            new TaskId(0, 0), List.of(), Map.of(), Instant.now()),
        new Snapshot.TaskSnapshotMetadata(
            new TaskId(0, 1), List.of(), Map.of(), Instant.now()),
        new Snapshot.TaskSnapshotMetadata(
            new TaskId(0, 2), List.of(), Map.of(), Instant.now())
    ));

    // when:
    final var snapshot = ctx.api.createSnapshot();

    // then:
    assertThat(snapshot.generation(), is(2L));
    assertThat(snapshot.state(), is(Snapshot.State.CREATED));
  }

  @Test
  public void shouldAddTaskSnapshotToSnapshot() {
    // given:
    ctx.api.createSnapshot();
    final var metadata = new Snapshot.TaskSnapshotMetadata(
        new TaskId(0, 0), List.of(), Map.of(), Instant.now());
    ctx.orchestrator.reportTaskSnapshotMetadata(1, List.of(metadata));

    // when:
    final var snapshot = ctx.store.currentSnapshot(true);

    // then:
    MatcherAssert.assertThat(snapshot.state(), is(Snapshot.State.CREATED));
    MatcherAssert.assertThat(snapshot.taskSnapshots(), contains(metadata));
  }

  @Test
  public void shouldFailAddTaskSnapshotIfConflictingMetadata() {
    // given:
    ctx.api.createSnapshot();
    final var metadata = new Snapshot.TaskSnapshotMetadata(
        new TaskId(0, 0),
        List.of(),
        Map.of("foo", "bar".getBytes(Charset.defaultCharset())),
        Instant.now()
    );
    ctx.orchestrator.reportTaskSnapshotMetadata(1, List.of(metadata));

    // when/then:
    assertThrows(
        RuntimeException.class,
        () -> ctx.orchestrator.reportTaskSnapshotMetadata(
            1,
            List.of(new Snapshot.TaskSnapshotMetadata(
                new TaskId(0, 0), List.of(), Map.of(), Instant.now()))
        )
    );
  }

  @Test
  public void shouldFailAddTaskSnapshotIfCompleted() {
    // given:
    ctx.api.createSnapshot();
    ctx.orchestrator.reportTaskSnapshotMetadata(1, List.of(
        new Snapshot.TaskSnapshotMetadata(
            new TaskId(0, 0), List.of(), Map.of(), Instant.now()),
        new Snapshot.TaskSnapshotMetadata(
            new TaskId(0, 1), List.of(), Map.of(), Instant.now()),
        new Snapshot.TaskSnapshotMetadata(
            new TaskId(0, 2), List.of(), Map.of(), Instant.now())
    ));

    // when/then:
    assertThrows(
        RuntimeException.class,
        () -> ctx.orchestrator.reportTaskSnapshotMetadata(
            1,
            List.of(new Snapshot.TaskSnapshotMetadata(
                new TaskId(0, 0), List.of(), Map.of(), Instant.now()))
        )
    );
  }

  @Test
  public void shouldCompleteSnapshot() {
    // given:
    ctx.api.createSnapshot();

    // when:
    ctx.orchestrator.reportTaskSnapshotMetadata(1, List.of(
        new Snapshot.TaskSnapshotMetadata(
            new TaskId(0, 0), List.of(), Map.of(), Instant.now()),
        new Snapshot.TaskSnapshotMetadata(
            new TaskId(0, 1), List.of(), Map.of(), Instant.now()),
        new Snapshot.TaskSnapshotMetadata(
            new TaskId(0, 2), List.of(), Map.of(), Instant.now())
    ));

    // then:
    MatcherAssert.assertThat(ctx.store.currentSnapshot(true).state(), is(Snapshot.State.COMPLETED));
  }

  @Test
  public void shouldFailSnapshot() {
    // given:
    ctx.api.createSnapshot();

    // when:
    ctx.orchestrator.failSnapshot(1);

    // then:
    MatcherAssert.assertThat(ctx.store.currentSnapshot(true).state(), is(Snapshot.State.FAILED));
  }

  @Test
  public void shouldFailFailSnapshotIfCompleted() {
    // given:
    ctx.api.createSnapshot();
    ctx.orchestrator.reportTaskSnapshotMetadata(1, List.of(
        new Snapshot.TaskSnapshotMetadata(
            new TaskId(0, 0), List.of(), Map.of(), Instant.now()),
        new Snapshot.TaskSnapshotMetadata(
            new TaskId(0, 1), List.of(), Map.of(), Instant.now()),
        new Snapshot.TaskSnapshotMetadata(
            new TaskId(0, 2), List.of(), Map.of(), Instant.now())
    ));
    MatcherAssert.assertThat(ctx.store.currentSnapshot(true).state(), is(Snapshot.State.COMPLETED));

    // when/then:
    assertThrows(RuntimeException.class, () -> ctx.orchestrator.failSnapshot(1));
  }

  private TestCtx createCtx() {
    return new TestCtx(topic);
  }

  private static class TestCtx {
    private final TopicSnapshotStore store;
    private final SnapshotOrchestrator orchestrator;
    private final SnapshotApi api;

    private TestCtx(final String topic) {
      this.store = TopicSnapshotStore.create(
          topic,
          (short) 1,
          Map.of(
              BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
              StreamsConfig.APPLICATION_ID_CONFIG, topic
          ),
          true
      );
      this.orchestrator = new LocalSnapshotOrchestrator(this.store, TASKS);
      this.api = new LocalSnapshotApi(this.store);
    }
  }
}