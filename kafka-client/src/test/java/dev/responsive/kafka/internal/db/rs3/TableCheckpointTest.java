package dev.responsive.kafka.internal.db.rs3;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import dev.responsive.kafka.internal.db.rs3.client.PssCheckpoint;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TableCheckpointTest {

  @Test
  public void shouldSerializeTableCheckpoint() {
    // given:
    final var storeId = UUID.randomUUID();
    final TableCheckpoint checkpoint = new TableCheckpoint(
        List.of(
            new TableCheckpoint.TablePssCheckpoint(
                Optional.of(123L),
                new PssCheckpoint(
                    storeId,
                    10,
                    new PssCheckpoint.SlateDbStorageCheckpoint(
                        "/foo/bar/10",
                        UUID.randomUUID()
                    )
                )
            ),
            new TableCheckpoint.TablePssCheckpoint(
                Optional.empty(),
                new PssCheckpoint(
                    storeId,
                    11,
                    new PssCheckpoint.SlateDbStorageCheckpoint(
                        "/foo/bar/11",
                        UUID.randomUUID()
                    )
                )
            )
        )
    );

    // when:
    final byte[] serialized = TableCheckpoint.serialize(checkpoint);
    System.out.println(new String(serialized, Charset.defaultCharset()));

    // then:
    final TableCheckpoint deserialized = TableCheckpoint.deserialize(serialized);
    assertThat(deserialized, is(checkpoint));
  }
}