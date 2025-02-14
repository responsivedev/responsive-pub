package dev.responsive.kafka.api.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ResponsiveConfigTest {

  @Test
  public void testRs3TableMapping() {
    String l1 = UUID.randomUUID().toString();
    String l2 = UUID.randomUUID().toString();

    Properties props = new Properties();
    props.setProperty(ResponsiveConfig.RS3_LOGICAL_STORE_MAPPING_CONFIG, "t1:" + l1 + ",t2:" + l2);

    final ResponsiveConfig config = ResponsiveConfig.responsiveConfig(props);
    Map<String, String> expectedMapping = new HashMap<>();
    expectedMapping.put("t1", l1);
    expectedMapping.put("t2", l2);
    assertEquals(expectedMapping, config.getMap(ResponsiveConfig.RS3_LOGICAL_STORE_MAPPING_CONFIG));
  }

  @Test
  public void testRs3RetryTimeoutConfig() {
    var props = new Properties();
    var config = ResponsiveConfig.responsiveConfig(props);
    assertEquals(
        ResponsiveConfig.RS3_RETRY_TIMEOUT_DEFAULT,
        config.getLong(ResponsiveConfig.RS3_RETRY_TIMEOUT_CONFIG)
    );

    props.setProperty(ResponsiveConfig.RS3_RETRY_TIMEOUT_CONFIG, "10");
    var reconfig = ResponsiveConfig.responsiveConfig(props);
    assertEquals(
        10L,
        reconfig.getLong(ResponsiveConfig.RS3_RETRY_TIMEOUT_CONFIG)
    );
  }

}