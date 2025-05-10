package dev.responsive.kafka.internal.snapshot.topic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import dev.responsive.kafka.internal.snapshot.Snapshot;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.jupiter.api.Test;

class SnapshotStoreSerdesTest {
  private final SnapshotStoreSerdes.SnapshotStoreRecordSerializer recordSerializer
      = new SnapshotStoreSerdes.SnapshotStoreRecordSerializer();
  private final SnapshotStoreSerdes.SnapshotStoreRecordDeserializer recordDeserializer
      = new SnapshotStoreSerdes.SnapshotStoreRecordDeserializer();
  private final SnapshotStoreSerdes.SnapshotStoreRecordKeySerializer keySerializer
      = new SnapshotStoreSerdes.SnapshotStoreRecordKeySerializer();
  private final SnapshotStoreSerdes.SnapshotStoreRecordKeyDeserializer keyDeserializer
      = new SnapshotStoreSerdes.SnapshotStoreRecordKeyDeserializer();

  @Test
  public void shouldSerializeRecord() {
    // given:
    final Snapshot snapshot = new Snapshot(
        Instant.now(),
        123,
        Snapshot.State.COMPLETED,
        List.of(
            new Snapshot.TaskSnapshotMetadata(
                new TaskId(0, 0),
                List.of(
                    new Snapshot.CommittedOffset("foo", 0, 100),
                    new Snapshot.CommittedOffset("bar", 0, 200)
                ),
                Map.of(
                    "store1", "store1-cp".getBytes(StandardCharsets.UTF_8),
                    "store2", "store2-cp".getBytes(StandardCharsets.UTF_8)
                ),
                Instant.now()
            ),
            new Snapshot.TaskSnapshotMetadata(
                new TaskId(1, 0),
                List.of(),
                Map.of(),
                Instant.now()
            )
        )
    );
    final SnapshotStoreRecord record
        = new SnapshotStoreRecord(SnapshotStoreRecordType.Snapshot, snapshot);

    // when:
    final byte[] serialized = recordSerializer.serialize("", record);
    System.out.println(new String(serialized));
    final SnapshotStoreRecord deserialized = recordDeserializer.deserialize("", serialized);

    // then:
    assertThat(deserialized, is(record));
  }

  @Test
  public void shouldSerializeKey() {
    // given:
    final SnapshotStoreRecordKey key
        = new SnapshotStoreRecordKey(SnapshotStoreRecordType.Snapshot, 100L);

    // when;
    final byte[] serialized = keySerializer.serialize("", key);
    System.out.println(new String(serialized));
    final SnapshotStoreRecordKey deserialized = keyDeserializer.deserialize("", serialized);

    // then:
    assertThat(deserialized, is(key));
  }
}