package dev.responsive.kafka.internal.snapshot;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.responsive.kafka.internal.clients.OffsetRecorder;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistration;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistry;
import dev.responsive.kafka.internal.utils.TopologyTaskInfo;
import dev.responsive.kafka.internal.utils.TopologyTaskInfoUtils;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SnapshotCommitListenerTest {
  private static final int SUBTOPOLOGIES = 1;
  private static final int TOPICS = 2;
  private static final int PARTITIONS = 2;
  private static final String STORE0_NAME = "store0";
  private static final byte[] TASK0_STORE0_CHECKPOINT
      = "task0topic0cp".getBytes(Charset.defaultCharset());

  private final TestSnapshotStore snapshotStore = new TestSnapshotStore();
  private final TopologyTaskInfo topologyTaskInfo = createTopologyTaskInfoWith(
      SUBTOPOLOGIES,
      TOPICS,
      PARTITIONS
  );
  private final GenerationStorage generationStorage
      = new SnapshotStoreBasedGenerationStorage(snapshotStore);
  private final LocalSnapshotOrchestrator orchestrator = new LocalSnapshotOrchestrator(
      snapshotStore,
      Set.copyOf(topologyTaskInfo.partitionsByTask().keySet())
  );
  private final SnapshotApi api = new LocalSnapshotApi(snapshotStore);
  private final AtomicReference<Instant> clock = new AtomicReference<>(Instant.now());

  @Mock
  private ResponsiveStoreRegistry storeRegistry;
  @Spy
  private SnapshotOrchestrator orchestratorSpy = orchestrator;
  @Mock
  private ResponsiveStoreRegistration task0t0storeRegistration;
  @Mock
  private ResponsiveStoreRegistration.StoreCallbacks task0t0storeCallbacks;
  @Mock
  private OffsetRecorder offsetRecorder;
  private long generation;

  private SnapshotCommitListener listener;

  @BeforeEach
  public void setup() {
    listener = new SnapshotCommitListener(
        orchestratorSpy,
        generationStorage,
        storeRegistry,
        topologyTaskInfo,
        offsetRecorder,
        clock::get
    );
    generation = api.createSnapshot().generation();
    lenient().when(task0t0storeRegistration.storeName()).thenReturn(STORE0_NAME);
    lenient().when(task0t0storeRegistration.callbacks()).thenReturn(task0t0storeCallbacks);
    lenient().when(task0t0storeCallbacks.checkpoint(any())).thenReturn(TASK0_STORE0_CHECKPOINT);
    lenient().when(storeRegistry.getRegisteredStoresForTask(eq(taskId(0, 0)), any()))
        .thenReturn(List.of(task0t0storeRegistration));
  }

  @Test
  public void shouldSkipTasksOnTheCurrentGeneration() {
    // given:
    orchestrator.reportTaskSnapshotMetadata(
        generation,
        List.of(new Snapshot.TaskSnapshotMetadata(
            taskId(0, 0), List.of(), Map.of(), Instant.now()))
    );

    // when:
    listener.onCommit(
        "foo",
        Map.of(
            recordingKey(partition(0, 0, 0)), 100L,
            recordingKey(partition(0, 1, 0)), 200L
        ),
        Map.of()
    );

    // then:
    verify(orchestratorSpy, times(0))
        .reportTaskSnapshotMetadata(anyLong(), any());
  }

  @Test
  public void shouldSkipTasksWithPartitionsThatDidNotCommit() {
    // when:
    listener.onCommit(
        "foo",
        Map.of(
            recordingKey(partition(0, 0, 0)), 100L
        ),
        Map.of()
    );

    // then:
    verify(orchestratorSpy, times(0))
        .reportTaskSnapshotMetadata(anyLong(), any());
  }

  @Test
  public void shouldTakeCheckpointOfStoreUsingSourceTopicOffset() {
    // given:
    when(task0t0storeRegistration.changelogTopicPartition())
        .thenReturn(partition(0, 0, 0));
    final TopicPartition partition0 = partition(0, 0, 0);
    final TopicPartition partition1 = partition(0, 1, 0);

    // when:
    listener.onCommit(
        "foo",
        Map.of(
            recordingKey(partition0), 100L,
            recordingKey(partition1), 200L
        ),
        Map.of()
    );

    // then:
    verify(task0t0storeCallbacks).checkpoint(Optional.of(100L));
    verify(orchestratorSpy).reportTaskSnapshotMetadata(
        generation,
        List.of(new Snapshot.TaskSnapshotMetadata(
            taskId(0, 0),
            List.of(
                new Snapshot.CommittedOffset(partition0.topic(), partition0.partition(), 100L),
                new Snapshot.CommittedOffset(partition1.topic(), partition1.partition(), 200L)
            ),
            Map.of(STORE0_NAME, TASK0_STORE0_CHECKPOINT),
            clock.get()
        ))
    );
  }

  @Test
  public void shouldTakeCheckpointOfStoreUsingChangelogOffset() {
    // given:
    final TopicPartition changelog = new TopicPartition("store0-changelog", 0);
    when(task0t0storeRegistration.changelogTopicPartition())
        .thenReturn(changelog);
    final TopicPartition partition0 = partition(0, 0, 0);
    final TopicPartition partition1 = partition(0, 1, 0);

    // when:
    listener.onCommit(
        "foo",
        Map.of(
            recordingKey(partition0), 100L,
            recordingKey(partition1), 200L
        ),
        Map.of(changelog, 300L)
    );

    // then:
    verify(task0t0storeCallbacks).checkpoint(Optional.of(300L));
    verify(orchestratorSpy).reportTaskSnapshotMetadata(
        generation,
        List.of(new Snapshot.TaskSnapshotMetadata(
            taskId(0, 0),
            List.of(
                new Snapshot.CommittedOffset(partition0.topic(), partition0.partition(), 100L),
                new Snapshot.CommittedOffset(partition1.topic(), partition1.partition(), 200L)
            ),
            Map.of(STORE0_NAME, TASK0_STORE0_CHECKPOINT),
            clock.get()
        ))
    );
  }

  @Test
  public void shouldTakeCheckpointOfStoreWithoutSpecifyingOffsetIfChangelogNotInCommit() {
    // given:
    final TopicPartition changelog = new TopicPartition("store0-changelog", 0);
    when(task0t0storeRegistration.changelogTopicPartition())
        .thenReturn(changelog);
    final TopicPartition partition0 = partition(0, 0, 0);
    final TopicPartition partition1 = partition(0, 1, 0);

    // when:
    listener.onCommit(
        "foo",
        Map.of(
            recordingKey(partition0), 100L,
            recordingKey(partition1), 200L
        ),
        Map.of()
    );

    // then:
    verify(task0t0storeCallbacks).checkpoint(Optional.empty());
    verify(orchestratorSpy).reportTaskSnapshotMetadata(
        generation,
        List.of(new Snapshot.TaskSnapshotMetadata(
            taskId(0, 0),
            List.of(
                new Snapshot.CommittedOffset(partition0.topic(), partition0.partition(), 100L),
                new Snapshot.CommittedOffset(partition1.topic(), partition1.partition(), 200L)
            ),
            Map.of(STORE0_NAME, TASK0_STORE0_CHECKPOINT),
            clock.get()
        ))
    );
  }

  @Test
  public void shouldNotThrowOnErrorFromSnapshotting() {
    // given:
    doThrow(new RuntimeException("oops"))
        .when(orchestratorSpy)
        .reportTaskSnapshotMetadata(anyLong(), any());
    final TopicPartition changelog = new TopicPartition("store0-changelog", 0);
    when(task0t0storeRegistration.changelogTopicPartition())
        .thenReturn(changelog);
    final TopicPartition partition0 = partition(0, 0, 0);
    final TopicPartition partition1 = partition(0, 1, 0);

    // when/then (no throw0:
    listener.onCommit(
        "foo",
        Map.of(
            recordingKey(partition0), 100L,
            recordingKey(partition1), 200L
        ),
        Map.of()
    );
    verify(orchestratorSpy).reportTaskSnapshotMetadata(anyLong(), any());
  }

  private OffsetRecorder.RecordingKey recordingKey(final TopicPartition topicPartition) {
    return new OffsetRecorder.RecordingKey(topicPartition, "");
  }

  private TaskId taskId(final int subtopology, final int partition) {
    return new TaskId(subtopology, partition);
  }

  private static TopicPartition partition(
      final int subtopology,
      final int topic,
      final int partition
  ) {
    return new TopicPartition(String.format("%s-%s", subtopology, topic), partition);
  }

  private static TopologyTaskInfo createTopologyTaskInfoWith(
      final int subtoplogies,
      final int topics,
      final int partitions
  ) {
    final Map<TopicPartition, TaskId> partitionToTask = new HashMap<>();
    final Map<TaskId, List<TopicPartition>> taskToPartitions = new HashMap<>();
    for (int s = 0; s < subtoplogies; s++) {
      for (int t = 0; t < topics; t++) {
        for (int p = 0; p < partitions; p++) {
          final TaskId taskId = new TaskId(s, p);
          taskToPartitions.putIfAbsent(new TaskId(s, p), new ArrayList<>(t));
          final TopicPartition tp = partition(s, t, p);
          taskToPartitions.get(taskId).add(tp);
          partitionToTask.put(tp, taskId);
        }
      }
    }
    return TopologyTaskInfoUtils.createWith(partitionToTask, taskToPartitions);
  }
}