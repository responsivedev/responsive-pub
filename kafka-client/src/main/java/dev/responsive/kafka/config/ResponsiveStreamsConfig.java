package dev.responsive.kafka.config;

import static dev.responsive.kafka.api.SharedClients.sharedClientConfigs;

import dev.responsive.db.CassandraClient;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.processor.internals.assignment.StickyTaskAssignor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponsiveStreamsConfig {

  public static final Logger LOG = LoggerFactory.getLogger(ResponsiveStreamsConfig.class);

  public static final int NUM_STANDBYS_OVERRIDE = 0;
  public static final String TASK_ASSIGNOR_CLASS_OVERRIDE = StickyTaskAssignor.class.getName();

  public static StreamsConfig verifiedStreamsConfigs(
      final Map<String, Object> configs,
      final CassandraClient cassandraClient,
      final Admin admin,
      final ScheduledExecutorService executor
  ) {
    final Properties propsWithOverrides = new Properties();
    propsWithOverrides.putAll(configs);
    propsWithOverrides.putAll(sharedClientConfigs(cassandraClient, admin, executor));

    // In this case the default and our desired value are both 0, so we only need to check for
    // accidental user overrides
    final Integer numStandbys = (Integer) configs.get(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG);
    if (numStandbys != null && numStandbys != 0) {
      final String errorMsg = String.format(
          "Invalid Streams configuration '%s': please override to '%d'",
          StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG,
          NUM_STANDBYS_OVERRIDE
      );
      LOG.error(errorMsg);
      throw new ConfigException(errorMsg);
    }

    // TODO(sophie): finish writing KIP to make this a public StreamsConfig, it's a bit awkward to
    //  be asking users to set an internal config and very very rare to be set to anything else
    final String taskAssignor = (String) configs.get(InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS);
    if (!TASK_ASSIGNOR_CLASS_OVERRIDE.equals(taskAssignor)) {
      final String errorMsg = String.format(
          "Invalid Streams configuration '%s': please override to '%s'",
          InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS,
          TASK_ASSIGNOR_CLASS_OVERRIDE
      );
      LOG.error(errorMsg);
      throw new ConfigException(errorMsg);
    } else {
      propsWithOverrides.put(InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS, TASK_ASSIGNOR_CLASS_OVERRIDE);
    }

    return new StreamsConfig(propsWithOverrides);
  }

}
