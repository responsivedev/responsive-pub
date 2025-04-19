package dev.responsive.kafka.api.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class ResponsiveConfigTest {

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