package dev.responsive.examples.async;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

public class ExampleApplicationMain {

  public static void main(final String[] args) throws IOException, InterruptedException {
    final Properties props = new Properties();
    try (final FileInputStream in = new FileInputStream(args[0])) {
      props.load(in);
    }
    final Topology topology = ExampleApplication.buildTopology();
    System.out.println(topology.describe().toString());
    try (final KafkaStreams streams = new KafkaStreams(topology, props)) {
      streams.start();
      final CountDownLatch latch = new CountDownLatch(1);
      Runtime.getRuntime().addShutdownHook(new Thread(latch::countDown));
      latch.await();
    }
  }
}
