package dev.responsive.kafka.internal.snapshot;

import com.google.common.annotations.VisibleForTesting;
import dev.responsive.kafka.internal.clients.OffsetRecorder;
import dev.responsive.kafka.internal.stores.ResponsiveStoreRegistry;
import dev.responsive.kafka.internal.utils.TopologyTaskInfo;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotCommitListener {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotCommitListener.class);

  private final SnapshotOrchestrator orchestrator;
  private final GenerationStorage generationStorage;
  private final ResponsiveStoreRegistry storeRegistry;
  private final TopologyTaskInfo topologyTaskInfo;
  private final Supplier<Instant> clock;

  public SnapshotCommitListener(
      final SnapshotOrchestrator orchestrator,
      final GenerationStorage generationStorage,
      final ResponsiveStoreRegistry storeRegistry,
      final TopologyTaskInfo topologyTaskInfo,
      final OffsetRecorder offsetRecorder
  ) {
    this(
        orchestrator,
        generationStorage,
        storeRegistry,
        topologyTaskInfo,
        offsetRecorder,
        Instant::now
    );
  }

  SnapshotCommitListener(
      final SnapshotOrchestrator orchestrator,
      final GenerationStorage generationStorage,
      final ResponsiveStoreRegistry storeRegistry,
      final TopologyTaskInfo topologyTaskInfo,
      final OffsetRecorder offsetRecorder,
      final Supplier<Instant> clock
  ) {
    this.orchestrator = Objects.requireNonNull(orchestrator);
    this.generationStorage = Objects.requireNonNull(generationStorage);
    this.storeRegistry = Objects.requireNonNull(storeRegistry);
    this.topologyTaskInfo = Objects.requireNonNull(topologyTaskInfo);
    this.clock = Objects.requireNonNull(clock);
    offsetRecorder.addCommitCallback(this::onCommit);
  }

  @VisibleForTesting
  void onCommit(
      final String threadId,
      final Map<OffsetRecorder.RecordingKey, Long> recordedCommittedOffsetsFromCommit,
      final Map<TopicPartition, Long> writtenOffsetsFromCommit
  ) {
    try {
      maybeTakeTaskSnapshots(
          threadId,
          recordedCommittedOffsetsFromCommit,
          writtenOffsetsFromCommit
      );
    } catch (final RuntimeException e) {
      LOG.warn("error taking task snapshots", e);
    }
  }

  private void maybeTakeTaskSnapshots(
      final String threadId,
      final Map<OffsetRecorder.RecordingKey, Long> recordedCommittedOffsetsFromCommit,
      final Map<TopicPartition, Long> writtenOffsetsFromCommit
  ) {
    final Map<TopicPartition, Long> committedOffsetsFromCommit = recordedCommittedOffsetsFromCommit
        .entrySet()
        .stream()
        .collect(Collectors.toMap(e -> e.getKey().getPartition(), Map.Entry::getValue));
    // get all tasks represented in the commit and snapshot them
    // TODO(snapshots): this means we won't ever snapshot a task if it's idle, or if its partitions
    //       don't commit together. This means that (in the former case for certain) we
    //       we may not ever finish the snapshot. We should do a few things:
    //       (1) move the snapshot execution somewhere else and trigger it when either
    //           commits happen or when we observe the app is totally idle. Alternatively
    //           we could trigger a commit for all tasks when the generation bumps, but
    //           that requires a kip to trigger commits.
    //       (2) derive the set of tasks to snapshot from the assignment rather than whats
    //           in the commit.
    //       (3) to do the above we need to make sure to filter out restoring tasks
    final Set<TaskId> tasksInCommit = committedOffsetsFromCommit.keySet()
        .stream()
        .map(p -> topologyTaskInfo.tasksByPartition().get(p))
        .collect(Collectors.toSet());

    final long generation = orchestrator.getCurrentGeneration();

    // go task by task
    final List<Snapshot.TaskSnapshotMetadata> taskSnapshots = new ArrayList<>(tasksInCommit.size());
    for (final TaskId taskId : tasksInCommit) {
      final long taskGeneration = generationStorage.lookupGeneration(taskId);
      if (taskGeneration >= generation) {
        // we already have a snapshot for this task, skip
        continue;
      }
      final List<TopicPartition> partitionsInTask = topologyTaskInfo.partitionsByTask().get(taskId);
      final List<Snapshot.CommittedOffset> snapshotOffsets
          = new ArrayList<>(partitionsInTask.size());
      final Set<TopicPartition> partitionsNotInCommit = new HashSet<>();
      for (final TopicPartition p : partitionsInTask) {
        if (committedOffsetsFromCommit.containsKey(p)) {
          snapshotOffsets.add(new Snapshot.CommittedOffset(
              p.topic(),
              p.partition(),
              committedOffsetsFromCommit.get(p)
          ));
        } else {
          partitionsNotInCommit.add(p);
        }
      }
      if (!partitionsNotInCommit.isEmpty()) {
        // we can only take the snapshot if the commit includes all partitions from the task
        // if it doesn't we don't know the source offset. In that case, wait for a later
        // commit
        LOG.warn("commit missing partitions from task {}: {}. Skip snapshot for this commit",
            taskId,
            partitionsNotInCommit.stream()
                .map(TopicPartition::toString)
                .collect(Collectors.joining(", "))
        );
        continue;
      }
      // checkpoint stores
      final Map<String, byte[]> storeCheckpoints = new HashMap<>();
      final var stores = storeRegistry.getRegisteredStoresForTask(taskId, threadId);
      for (final var store : stores) {
        // get the store's changelog offset from either the written or committed offsets
        Optional<Long> changelogOffset;
        if (committedOffsetsFromCommit.containsKey(store.changelogTopicPartition())) {
          // for stores sourced from a source topic
          changelogOffset
              = Optional.of(committedOffsetsFromCommit.get(store.changelogTopicPartition()));
        } else if (writtenOffsetsFromCommit.containsKey(store.changelogTopicPartition())) {
          // for stores with an internal changelog topic
          changelogOffset
              = Optional.of(writtenOffsetsFromCommit.get(store.changelogTopicPartition()));
        } else {
          // the task's source offset advanced, but it didn't write anything
          changelogOffset = Optional.empty();
        }
        final var checkpoint = store.callbacks().checkpoint(changelogOffset);
        storeCheckpoints.put(store.storeName(), checkpoint);
      }
      final Snapshot.TaskSnapshotMetadata tsm = new Snapshot.TaskSnapshotMetadata(
          taskId,
          snapshotOffsets,
          storeCheckpoints,
          clock.get()
      );
      taskSnapshots.add(tsm);
    }
    if (!taskSnapshots.isEmpty()) {
      orchestrator.reportTaskSnapshotMetadata(generation, taskSnapshots);
    }
  }

  public void close() {
    orchestrator.close();
  }
}
