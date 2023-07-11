package dev.responsive.kafka.store;

import dev.responsive.kafka.config.ResponsiveConfig;
import java.time.Duration;

public class FlushTriggers {
  public static FlushTriggers ALWAYS = new FlushTriggers(0, 0, Duration.ZERO);

  private final int records;
  private final long bytes;
  private final Duration interval;

  public static FlushTriggers fromConfig(final ResponsiveConfig config) {
    return new FlushTriggers(
        config.getInt(ResponsiveConfig.STORE_FLUSH_RECORDS_TRIGGER),
        config.getLong(ResponsiveConfig.STORE_FLUSH_BYTES_TRIGGER),
        Duration.ofMillis(config.getLong(ResponsiveConfig.STORE_FLUSH_INTERVAL_TRIGGER))
    );
  }

  static FlushTriggers ofRecords(final int records) {
    return new FlushTriggers(records, Long.MAX_VALUE, Duration.ofMillis(Long.MAX_VALUE));
  }

  static FlushTriggers ofBytes(final long bytes) {
    return new FlushTriggers(Integer.MAX_VALUE, bytes, Duration.ofMillis(Long.MAX_VALUE));
  }

  static FlushTriggers ofInterval(final Duration interval) {
    return new FlushTriggers(Integer.MAX_VALUE, Long.MAX_VALUE, interval);
  }

  private FlushTriggers(final int records, final long bytes, final Duration interval) {
    this.records = records;
    this.bytes = bytes;
    this.interval = interval;
  }

  public int getRecords() {
    return records;
  }

  public long getBytes() {
    return bytes;
  }

  public Duration getInterval() {
    return interval;
  }
}
