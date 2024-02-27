package dev.responsive.kafka.internal.db.mongo;

import static dev.responsive.kafka.api.config.ResponsiveConfig.STORAGE_HOSTNAME_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;

import com.mongodb.client.MongoClient;
import dev.responsive.kafka.api.config.StorageBackend;
import dev.responsive.kafka.internal.db.partitioning.WindowSegmentPartitioner;
import dev.responsive.kafka.internal.utils.SessionUtil;
import dev.responsive.kafka.internal.utils.WindowedKey;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import java.util.Map;
import org.apache.kafka.common.utils.Bytes;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

class MongoWindowedTableTest {

  @RegisterExtension
  public static final ResponsiveExtension EXT = new ResponsiveExtension(StorageBackend.MONGO_DB);
  private static final CollectionCreationOptions UNSHARDED = new CollectionCreationOptions(
      false,
      0
  );
  private static final byte[] DEFAULT_VALUE = new byte[] {1};

  private String name;
  private MongoClient client;

  @BeforeEach
  public void before(
      final TestInfo info,
      @ResponsiveConfigParam final Map<String, Object> props
  ) {
    name = info.getDisplayName().replace("()", "");

    final String mongoConnection = (String) props.get(STORAGE_HOSTNAME_CONFIG);
    client = SessionUtil.connect(mongoConnection, null, null);
  }

  @Test
  public void shouldSucceedSimpleSetGet() {
    // Given:
    final WindowSegmentPartitioner partitioner = new WindowSegmentPartitioner(10_000L, 1_000L);
    final var segment = partitioner.segmenter().activeSegments(0, 100).get(0);

    final MongoWindowedTable table = new MongoWindowedTable(client, name, partitioner,
        false, UNSHARDED
    );
    final var flushManager = table.init(0);
    flushManager.updateOffsetAndStreamTime(0, 100);
    flushManager.createSegment(segment);

    // When:
    final var byteKey = Bytes.wrap("key".getBytes());
    var writer = flushManager.createWriter(segment);
    writer.insert(
        new WindowedKey(byteKey, 0),
        DEFAULT_VALUE,
        table.localEpoch(0)
    );
    writer.flush();

    // Then:
    var value = table.fetch(0, byteKey, 0);
    assertThat(value, Matchers.equalTo(DEFAULT_VALUE));
    value = table.fetch(0, byteKey, 100);
    assertThat(value, Matchers.nullValue());
    value = table.fetch(0, Bytes.wrap("other".getBytes()), 0);
    assertThat(value, Matchers.nullValue());
  }
}