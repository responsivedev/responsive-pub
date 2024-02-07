package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import java.time.Duration;
import java.util.Random;

public class FlushTriggers {
  public static FlushTriggers ALWAYS = new FlushTriggers(0, 0, Duration.ZERO, Duration.ZERO);
  private static final Random RANDOM = new Random();

  private final int records;
  private final long bytes;
  private long intervalOffset;
  private final Duration interval;
  private final Duration jitter;

  public static FlushTriggers fromConfig(final ResponsiveConfig config) {
    return new FlushTriggers(
        config.getInt(ResponsiveConfig.STORE_FLUSH_RECORDS_TRIGGER_CONFIG),
        config.getLong(ResponsiveConfig.STORE_FLUSH_BYTES_TRIGGER_CONFIG),
        Duration.ofMillis(config.getLong(ResponsiveConfig.STORE_FLUSH_INTERVAL_TRIGGER_MS_CONFIG)),
        Duration.ofMillis(
            config.getInt(ResponsiveConfig.STORE_FLUSH_INTERVAL_TRIGGER_JITTER_CONFIG))
    );
  }

  static FlushTriggers ofRecords(final int records) {
    return new FlushTriggers(
        records,
        Long.MAX_VALUE,
        Duration.ofMillis(Long.MAX_VALUE),
        Duration.ZERO
    );
  }

  static FlushTriggers ofBytes(final long bytes) {
    return new FlushTriggers(
        Integer.MAX_VALUE,
        bytes,
        Duration.ofMillis(Long.MAX_VALUE),
        Duration.ZERO
    );
  }

  static FlushTriggers ofInterval(final Duration interval) {
    return new FlushTriggers(Integer.MAX_VALUE, Long.MAX_VALUE, interval, Duration.ZERO);
  }

  static FlushTriggers ofInterval(final Duration interval, final Duration jitter) {
    return new FlushTriggers(Integer.MAX_VALUE, Long.MAX_VALUE, interval, jitter);
  }

  private FlushTriggers(
      final int records,
      final long bytes,
      final Duration interval,
      final Duration jitter
  ) {
    this.records = records;
    this.bytes = bytes;
    this.interval = interval;
    this.jitter = jitter;
    reset();
  }

  public int getRecords() {
    return records;
  }

  public long getBytes() {
    return bytes;
  }

  public Duration getInterval() {
    return interval.plusMillis(intervalOffset);
  }

  public void reset() {
    if (interval.isZero() || jitter.isZero()) {
      intervalOffset = 0;
    } else {
      intervalOffset = RANDOM.nextLong() % (jitter.toMillis());
      intervalOffset = RANDOM.nextBoolean() ? intervalOffset : -1 * intervalOffset;
    }
  }
}
