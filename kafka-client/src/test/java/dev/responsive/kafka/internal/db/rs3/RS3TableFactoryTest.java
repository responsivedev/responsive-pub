package dev.responsive.kafka.internal.db.rs3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import java.util.Collections;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class RS3TableFactoryTest {

  @Test
  public void testTableMapping() {
    String table = "test-table";
    String uuid = "b1a45157-e2f0-4698-be0e-5bf3a9b8e9d1";

    ResponsiveConfig config = Mockito.mock(ResponsiveConfig.class);
    Mockito.when(config.getMap(ResponsiveConfig.RS3_LOGICAL_STORE_MAPPING_CONFIG))
        .thenReturn(Collections.singletonMap(table, uuid));

    final RS3TableFactory factory = new RS3TableFactory("localhost", 9888);
    final RS3KVTable rs3Table = (RS3KVTable) factory.kvTable(table, config);
    assertEquals(uuid, rs3Table.storedId().toString());
  }

  @Test
  public void testMissingTableMapping() {
    String table = "test-table";
    ResponsiveConfig config = Mockito.mock(ResponsiveConfig.class);
    Mockito.when(config.getMap(ResponsiveConfig.RS3_LOGICAL_STORE_MAPPING_CONFIG))
        .thenReturn(Collections.emptyMap());

    final RS3TableFactory factory = new RS3TableFactory("localhost", 9888);
    assertThrows(ConfigException.class, () -> factory.kvTable(table, config));
  }

}