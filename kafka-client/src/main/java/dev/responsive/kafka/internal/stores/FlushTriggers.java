package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import java.time.Duration;
import java.util.Random;

public class FlushTriggers {
  public static FlushTriggers ALWAYS = new FlushTriggers(0, 0, Duration.ZERO);
  private final Random RANDOM = new Random();

  private final int records;
  private final long bytes;
  private long off;
  private final Duration interval;

  public static FlushTriggers fromConfig(final ResponsiveConfig config) {
    return new FlushTriggers(
        config.getInt(ResponsiveConfig.STORE_FLUSH_RECORDS_TRIGGER_CONFIG),
        config.getLong(ResponsiveConfig.STORE_FLUSH_BYTES_TRIGGER_CONFIG),
        Duration.ofMillis(config.getLong(ResponsiveConfig.STORE_FLUSH_INTERVAL_TRIGGER_MS_CONFIG))
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
    resetOff();
  }

  public int getRecords() {
    return records;
  }

  public long getBytes() {
    return bytes;
  }

  public Duration getInterval() {
    return interval.plusMillis(off);
  }

  public void resetOff() {
    if (interval.toMillis() == 0) {
      off = 0;
    } else {
      off = RANDOM.nextLong() % ((long) (interval.toMillis() * 0.75));
      off = RANDOM.nextBoolean() ? off : -1 * off;
    }
  }
}
