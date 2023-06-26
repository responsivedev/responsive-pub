package dev.responsive.kafka.api;

import java.io.Closeable;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 * The application driver for a single Kafka Streams application connected to the Responsive client. This
 * cannot be reused for different applications with different application ids and must always be closed,
 * although you can do so either by directly invoking {@link #close} or indirectly by just closing the
 * associated {@link KafkaStreams} returned from {@link #kafkaStreams(Topology)} which cleans up for you
 * <p>
 * This driver can be used to obtain a {@link StreamsStoreDriver} via the {@link #storeDriver()} API, which
 * in turn allows you to swap in any flavor of StateStore backed by Responsive. See the {@link StreamsStoreDriver}
 * interface for more details.
 * <p>
 * Once you have built a {@link Topology} using the provided {@link StreamsStoreDriver}, you can pass it
 * into the {@link #kafkaStreams(Topology)} API to get the {@link KafkaStreams} for this application. It is
 * your responsibility to manage the lifecycle of the returned object. Check out the javadocs for exact info
 * <p>
 * Note that Responsive may override some configs to ensure the optimal experience. You can always view the
 * final application configuration by inspecting the contents returned by {@link #streamsConfig()}
 */
public interface StreamsApplicationDriver extends Closeable {

  /**
   * @return the StreamsConfig that is used to construct this KafkaStreams application instance
   */
  StreamsConfig streamsConfig();

  /**
   * @return the store driver associated with this KafkaStreams application instance.
   * See {@link StreamsStoreDriver}
   */
  StreamsStoreDriver storeDriver();

  /**
   *
   * @param topology the {@link Topology} to execute. Use the {@link #storeDriver()} to insert Responsive state stores
   *                 into your application topology.
   * @return the KafkaStreams configured to run with Responsive state stores
   * <p>
   * NOTE: this driver does not start or close the {@link KafkaStreams} object, so you retain full control over it.
   * with all {@code KafkaStreams} instances, you must always close it even if it was never started
   */
  KafkaStreams kafkaStreams(Topology topology);

}
