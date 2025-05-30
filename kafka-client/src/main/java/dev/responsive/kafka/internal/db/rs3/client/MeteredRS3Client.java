package dev.responsive.kafka.internal.db.rs3.client;

import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes.CreateStoreOptions;
import dev.responsive.kafka.internal.db.rs3.client.CreateStoreTypes.CreateStoreResult;
import dev.responsive.kafka.internal.metrics.ResponsiveMetrics;
import dev.responsive.kafka.internal.utils.WindowedKey;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public class MeteredRS3Client implements RS3Client {
  public static final String GROUP_NAME = "rs3-table-metrics";

  private static final String GET_SENSOR_NAME = "get-sensor";
  private static final String GET_LATENCY_NS_AVG = "get-latency-ns-avg";
  private static final String GET_LATENCY_NS_AVG_DESC = "average rs3 get latency in nanos";

  private final RS3Client delegate;

  private final ResponsiveMetrics metrics;
  private final Sensor getSensor;

  public MeteredRS3Client(
      final RS3Client delegate,
      final ResponsiveMetrics metrics,
      final ResponsiveMetrics.MetricScopeBuilder metricsScopeBuilder
  ) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    ResponsiveMetrics.MetricScope metricScope = metricsScopeBuilder.build(GROUP_NAME);
    this.getSensor = this.metrics.addSensor(metricScope.sensorName(GET_SENSOR_NAME));
    getSensor.add(
        metrics.metricName(GET_LATENCY_NS_AVG, GET_LATENCY_NS_AVG_DESC, metricScope),
        new Avg()
    );
  }

  @Override
  public CurrentOffsets getCurrentOffsets(
      final UUID storeId,
      final LssId lssId,
      final int pssId
  ) {
    return delegate.getCurrentOffsets(storeId, lssId, pssId);
  }

  @Override
  public StreamSenderMessageReceiver<WalEntry, Optional<Long>> writeWalSegmentAsync(
      final UUID storeId,
      final LssId lssId,
      final int pssId,
      final Optional<Long> expectedWrittenOffset,
      final long endOffset
  ) {
    return delegate.writeWalSegmentAsync(storeId, lssId, pssId, expectedWrittenOffset, endOffset);
  }

  @Override
  public Optional<Long> writeWalSegment(
      final UUID storeId,
      final LssId lssId,
      final int pssId,
      final Optional<Long> expectedWrittenOffset,
      final long endOffset,
      final List<WalEntry> entries
  ) {
    return delegate.writeWalSegment(
        storeId,
        lssId,
        pssId,
        expectedWrittenOffset,
        endOffset,
        entries
    );
  }

  @Override
  public Optional<byte[]> get(
      final UUID storeId,
      final LssId lssId,
      final int pssId,
      final Optional<Long> expectedWrittenOffset,
      final Bytes key
  ) {
    final Instant start = Instant.now();
    final Optional<byte[]> result = delegate.get(
        storeId,
        lssId,
        pssId,
        expectedWrittenOffset,
        key
    );
    getSensor.record(Duration.between(start, Instant.now()).toNanos());
    return result;
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(
      final UUID storeId,
      final LssId lssId,
      final int pssId,
      final Optional<Long> expectedWrittenOffset,
      final Range<Bytes> range
  ) {
    return delegate.range(
        storeId,
        lssId,
        pssId,
        expectedWrittenOffset,
        range
    );
  }

  @Override
  public Optional<byte[]> windowedGet(
      final UUID storeId,
      final LssId lssId,
      final int pssId,
      final Optional<Long> expectedWrittenOffset,
      final WindowedKey key
  ) {
    final Instant start = Instant.now();
    final Optional<byte[]> result = delegate.windowedGet(
        storeId,
        lssId,
        pssId,
        expectedWrittenOffset,
        key
    );
    getSensor.record(Duration.between(start, Instant.now()).toNanos());
    return result;
  }

  @Override
  public KeyValueIterator<WindowedKey, byte[]> windowedRange(
      final UUID storeId,
      final LssId lssId,
      final int pssId,
      final Optional<Long> expectedWrittenOffset,
      final Range<WindowedKey> range
  ) {
    return delegate.windowedRange(
        storeId,
        lssId,
        pssId,
        expectedWrittenOffset,
        range
    );
  }

  @Override
  public List<StoreInfo> listStores() {
    return delegate.listStores();
  }

  @Override
  public CreateStoreResult createStore(
      final String storeName,
      final CreateStoreOptions options
  ) {
    return delegate.createStore(storeName, options);
  }

  @Override
  public PssCheckpoint createCheckpoint(
      final UUID storeId,
      final LssId lssId,
      final int pssId,
      final Optional<Long> expectedWrittenOffset) {
    return delegate.createCheckpoint(storeId, lssId, pssId, expectedWrittenOffset);
  }

  public void close() {
    this.metrics.removeSensor(GET_SENSOR_NAME);
  }
}
