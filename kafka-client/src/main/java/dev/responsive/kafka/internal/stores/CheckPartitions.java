package dev.responsive.kafka.internal.stores;

import dev.responsive.kafka.api.config.ResponsiveConfig;
import dev.responsive.kafka.api.stores.ResponsiveKeyValueParams;
import dev.responsive.kafka.internal.db.CassandraClientFactory;
import dev.responsive.kafka.internal.db.CassandraTableSpecFactory;
import dev.responsive.kafka.internal.db.DefaultCassandraClientFactory;
import dev.responsive.kafka.internal.db.partitioning.TablePartitioner;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

public class CheckPartitions {
  public static void main(String... args) {
    final String changelog = "responsive-example-deduper-scale-partitions-state-store-changelog";
    final List<TopicPartitionInfo> partitions;
    final Map<Integer, Long> endOffsets = new HashMap<>();
    final String storeName = "state-store";

    final Properties properties = new Properties();
    try {
      final InputStream in = CheckPartitions.class.getClassLoader()
          .getResourceAsStream("checkpart.properties");
      properties.load(in);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }

    try (Admin admin = Admin.create(properties)) {
      final var descriptions = admin.describeTopics(List.of(changelog)).allTopicNames().get();
      partitions = descriptions.get(changelog).partitions();
      final var listOffsetsResult = admin.listOffsets(partitions.stream().collect(Collectors.toMap(
          i -> new TopicPartition(changelog, i.partition()),
          i -> OffsetSpec.latest()
      ))).all().get();
      for (final var e : listOffsetsResult.entrySet()) {
        endOffsets.put(e.getKey().partition(), e.getValue().offset());
      }
    } catch (final InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    final ResponsiveConfig cfg = ResponsiveConfig.responsiveConfig(properties);
    final CassandraClientFactory cassandraFactory = new DefaultCassandraClientFactory();
    try (final var cqlSession = cassandraFactory.createCqlSession(cfg)) {
      final var client = cassandraFactory.createClient(cqlSession, cfg);
      final var params = ResponsiveKeyValueParams.fact(storeName)
          .withTimeToLive(Duration.ofDays(30));
      final var spec = CassandraTableSpecFactory
          .fromKVParams(params, TablePartitioner.defaultPartitioner());
      final var tbl = client.factFactory().create(spec);
      for (final Integer p : endOffsets.keySet()) {
        final long savedOffset = tbl.fetchOffset(p);
        System.out.printf("offset(%d) end(%d) saved(%d)%n", p, endOffsets.get(p), savedOffset);
      }
    } catch (final InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }
}
