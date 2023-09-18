package dev.responsive.utils;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StoreUtilTest {

  @Test
  public void shouldThrowOnCompactAndTruncateChangelogTrue() {
    assertThrows(
        IllegalArgumentException.class,
        () -> StoreUtil.validateLogConfigs(Map.of(
            TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT
        ), true, "store")
    );
  }

  @Test
  public void shouldNotThrowOnDeleteAndTruncateChangelogTrue() {
    StoreUtil.validateLogConfigs(Map.of(
        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE
    ), true, "store");
  }

  @Test
  public void shouldNotThrowOnCompactDeleteAndTruncateChangelogTrue() {
    StoreUtil.validateLogConfigs(Map.of(
        TopicConfig.CLEANUP_POLICY_CONFIG,
        TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE
    ), true, "store");
  }

  @Test
  public void shouldNotThrowWhenCleanupPolicyUnspecifiedAndTruncateChangelogTrue() {
    StoreUtil.validateLogConfigs(Collections.emptyMap(), true, "store");
  }

  @Test
  public void shouldNotThrowOnCompactAndTruncateChangelogFalse() {
    StoreUtil.validateLogConfigs(Map.of(
        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT
    ), false, "store");
  }

  @Test
  public void shouldNotThrowOnDeleteAndTruncateChangelogFalse() {
    StoreUtil.validateLogConfigs(Map.of(
        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE
    ), false, "store");
  }

  @Test
  public void shouldNotThrowOnCompactDeleteAndTruncateChangelogFalse() {
    StoreUtil.validateLogConfigs(Map.of(
        TopicConfig.CLEANUP_POLICY_CONFIG,
        TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE
    ), false, "store");
  }

  @Test
  public void shouldNotThrowWhenCleanupPolicyUnspecifiedAndTruncateChangelogFalse() {
    StoreUtil.validateLogConfigs(Collections.emptyMap(), false, "store");
  }

}