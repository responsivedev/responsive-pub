package dev.responsive.utils;

import static dev.responsive.db.ColumnName.PARTITION_KEY;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import dev.responsive.db.CassandraClient;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;

/**
 * Use this any time we need to repeatedly look up metadata that doesn't change, so we can
 * cache the results rather than make admin/client requests every single time
 */
public class CachedMetadata {

  private final Admin admin;
  private final CassandraClient client;

  private final Map<String, Integer> changelogToKafkaPartitionCount = new HashMap<>();
  private final Map<String, OptionalInt> tableToSubPartitionCount = new HashMap<>();

  public CachedMetadata(final Admin admin, final CassandraClient client) {
    this.admin = admin;
    this.client = client;
  }

  public int kafkaPartitionCount(final String changelogTopicName) {
    if (changelogToKafkaPartitionCount.containsKey(changelogTopicName)) {
      return changelogToKafkaPartitionCount.get(changelogTopicName);
    } else {
      try {
        final int partitionCount = admin.describeTopics(List.of(changelogTopicName))
            .allTopicNames()
            .get()
            .get(changelogTopicName)
            .partitions()
            .size();
        changelogToKafkaPartitionCount.put(changelogTopicName, partitionCount);
        return partitionCount;
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public OptionalInt subPartitionCount(final String tableName) {
    if (tableToSubPartitionCount.containsKey(tableName)) {
      return tableToSubPartitionCount.get(tableName);
    } else {

      // TODO: is it faster to just write the actual remote partition count into cassandra?
      final ResultSet result = client.execute(
          String.format("SELECT DISTINCT %s FROM %s;", PARTITION_KEY.column(), tableName));

      final int numPartitions = result.all().size();
      final OptionalInt subPartitionCount = numPartitions == 0
          ? OptionalInt.empty()
          : OptionalInt.of(numPartitions);

      tableToSubPartitionCount.put(tableName, subPartitionCount);
      return subPartitionCount;
    }
  }
}
