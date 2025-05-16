package dev.responsive.kafka.internal.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LocalSnapshotOrchestratorTest {
  private static final TaskId TASK_0 = new TaskId(0, 0);
  private static final TaskId TASK_1 = new TaskId(0, 1);
  private static final TaskId TASK_2 = new TaskId(0, 2);

  private final TestSnapshotStore snapshotStore = new TestSnapshotStore();
  private final SnapshotApi api = new LocalSnapshotApi(snapshotStore);
  private long generation;
  private final LocalSnapshotOrchestrator orchestrator = new LocalSnapshotOrchestrator(
      snapshotStore,
      Set.of(TASK_0, TASK_1, TASK_2)
  );

  @BeforeEach
  public void setup() {
    final var snapshot = api.createSnapshot();
    generation = snapshot.generation();
  }

  @Test
  public void shouldFailUpdateWithConflictingTaskSnapshot() {
    // given:
    orchestrator.reportTaskSnapshotMetadata(
        generation,
        List.of(new Snapshot.TaskSnapshotMetadata(
            TASK_0,
            List.of(new Snapshot.CommittedOffset("foo", 0, 123L)),
            Map.of(),
            Instant.now()
        ))
    );

    // when/then:
    assertThrows(
        IllegalStateException.class,
        () -> orchestrator.reportTaskSnapshotMetadata(
            generation,
            List.of(new Snapshot.TaskSnapshotMetadata(
                TASK_0,
                List.of(new Snapshot.CommittedOffset("foo", 0, 456L)),
                Map.of(),
                Instant.now()
            ))
        )
    );
  }

  @Test
  public void shouldDoIdempotentTaskSnapshotUpdate() {
    // given:
    final var taskSnapshots = List.of(new Snapshot.TaskSnapshotMetadata(
        TASK_0,
        List.of(new Snapshot.CommittedOffset("foo", 0, 123L)),
        Map.of(),
        Instant.now()
    ));
    orchestrator.reportTaskSnapshotMetadata(generation, taskSnapshots);

    // when:
    orchestrator.reportTaskSnapshotMetadata(generation, taskSnapshots);

    // then:
    final var snapshot = api.getCurrentSnapshot();
    assertThat(snapshot.taskSnapshots(), is(taskSnapshots));
  }

  @Test
  public void shouldFailUpdateForWrongGeneration() {
    // when/then:
    assertThrows(RuntimeException.class,
        () -> orchestrator.reportTaskSnapshotMetadata(generation - 1, List.of()));
  }

  @Test
  public void shouldCompleteSnapshot() {
    // given:
    final var taskSnapshots = List.of(
        new Snapshot.TaskSnapshotMetadata(
            TASK_0,
            List.of(new Snapshot.CommittedOffset("foo", 0, 123L)),
            Map.of(),
            Instant.now()
        ),
        new Snapshot.TaskSnapshotMetadata(
            TASK_1,
            List.of(new Snapshot.CommittedOffset("foo", 1, 456L)),
            Map.of(),
            Instant.now()
        ),
        new Snapshot.TaskSnapshotMetadata(
            TASK_2,
            List.of(new Snapshot.CommittedOffset("foo", 2, 100L)),
            Map.of(),
            Instant.now()
        )
    );

    // when:
    orchestrator.reportTaskSnapshotMetadata(generation, taskSnapshots);

    // then:
    final var snapshot = api.getCurrentSnapshot();
    assertThat(snapshot.state(), is(Snapshot.State.COMPLETED));
    assertThat(snapshot.taskSnapshots(), is(taskSnapshots));
  }

  @Test
  public void shouldCompleteSnapshotWhenFinishedTasksInSeparateUpdates() {
    // given:
    final var taskSnapshots = List.of(
        new Snapshot.TaskSnapshotMetadata(
            TASK_0,
            List.of(new Snapshot.CommittedOffset("foo", 0, 123L)),
            Map.of(),
            Instant.now()
        )
    );
    final var taskSnapshots2 = List.of(
        new Snapshot.TaskSnapshotMetadata(
            TASK_1,
            List.of(new Snapshot.CommittedOffset("foo", 1, 456L)),
            Map.of(),
            Instant.now()
        ),
        new Snapshot.TaskSnapshotMetadata(
            TASK_2,
            List.of(new Snapshot.CommittedOffset("foo", 2, 100L)),
            Map.of(),
            Instant.now()
        )
    );
    orchestrator.reportTaskSnapshotMetadata(generation, taskSnapshots);

    // when:
    orchestrator.reportTaskSnapshotMetadata(generation, taskSnapshots2);

    // then:
    final var snapshot = api.getCurrentSnapshot();
    assertThat(snapshot.state(), is(Snapshot.State.COMPLETED));
    final var allTaskSnapshots = new ArrayList<>(taskSnapshots);
    allTaskSnapshots.addAll(taskSnapshots2);
    assertThat(snapshot.taskSnapshots(), is(allTaskSnapshots));
  }

  @Test
  public void shouldFailUpdateForCompletedSnapshot() {
    // given:
    final var taskSnapshots = List.of(
        new Snapshot.TaskSnapshotMetadata(
            TASK_0,
            List.of(new Snapshot.CommittedOffset("foo", 0, 123L)),
            Map.of(),
            Instant.now()
        ),
        new Snapshot.TaskSnapshotMetadata(
            TASK_1,
            List.of(new Snapshot.CommittedOffset("foo", 1, 456L)),
            Map.of(),
            Instant.now()
        ),
        new Snapshot.TaskSnapshotMetadata(
            TASK_2,
            List.of(new Snapshot.CommittedOffset("foo", 2, 100L)),
            Map.of(),
            Instant.now()
        )
    );
    orchestrator.reportTaskSnapshotMetadata(generation, taskSnapshots);

    // when/then:
    assertThrows(
        RuntimeException.class,
        () -> orchestrator.reportTaskSnapshotMetadata(generation, taskSnapshots));
  }

  @Test
  public void shouldGetCurrentGeneration() {
    assertThat(orchestrator.getCurrentGeneration(), is(generation));
  }
}