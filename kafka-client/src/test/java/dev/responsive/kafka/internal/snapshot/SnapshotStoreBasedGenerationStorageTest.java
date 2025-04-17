package dev.responsive.kafka.internal.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnapshotStoreBasedGenerationStorageTest {
  private static final TaskId TASK_0 = new TaskId(0, 0);

  private final TestSnapshotStore snapshotStore = new TestSnapshotStore();
  private final SnapshotApi api = new LocalSnapshotApi(snapshotStore);
  private final GenerationStorage generationStorage
      = new SnapshotStoreBasedGenerationStorage(snapshotStore);
  private long oldGeneration;
  private long generation;

  @BeforeEach
  public void setup() {
    oldGeneration = snapshotStore.currentSnapshot(true).generation();
    final var snapshot = api.createSnapshot();
    generation = snapshot.generation();
  }

  @Test
  public void shouldReturnNewGenerationForCompletedSnapshot() {
    // given:
    snapshotStore
        .updateCurrentSnapshot(s -> s.withTaskSnapshots(List.of(), Snapshot.State.COMPLETED));

    // when/then:
    assertThat(generationStorage.lookupGeneration(TASK_0), is(generation));
  }

  @Test
  public void shouldReturnNewGenerationForTaskWithCompletedTaskSnapshot() {
    // given:
    snapshotStore
        .updateCurrentSnapshot(
            s -> s.withTaskSnapshots(
                List.of(new Snapshot.TaskSnapshotMetadata(
                    TASK_0, List.of(), Map.of(), Instant.now())),
                s.state()
            ));

    // when/then:
    assertThat(generationStorage.lookupGeneration(TASK_0), is(generation));
  }

  @Test
  public void shouldReturnOldGenerationForTaskWithoutCompletedSnapshot() {
    assertThat(generationStorage.lookupGeneration(TASK_0), is(oldGeneration));
  }
}