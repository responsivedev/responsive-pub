package dev.responsive.kafka.internal.db.pocket;

import static dev.responsive.kafka.testutils.ResponsiveExtension.PocketContainer;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.testutils.ResponsiveConfigParam;
import dev.responsive.kafka.testutils.ResponsiveExtension;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ResponsiveExtension.class)
public class PocketKVTableIntegrationTest {
  private String testName;
  private ResponsiveConfig responsiveConfig;

  @BeforeEach
  public void setup(
      final TestInfo info,
      final PocketContainer pocketContainer,
      @ResponsiveConfigParam final ResponsiveConfig responsiveConfig
  ) {
    testName = info.getTestMethod().orElseThrow().getName();
    this.responsiveConfig = responsiveConfig;
  }

  @Test
  public void shouldReadWriteFromKVStore() {
    //final Topology topology =
  }
}
