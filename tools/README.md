# Responsive Tools

This module contains tools that can be helpful when using the Responsive
Platform. This module builds a Fat JAR containing all dependencies required
to run the tools. Once you have the JAR, you can run any of its commands -
the example below runs `StreamsBytecodeAnalyzer`:
```bash
java -cp tools/build/libs/tools-fat-0.1.0-1-SNAPSHOT.jar dev.responsive.tools.analyzer.StreamsBytecodeAnalyzer --help
Usage: analyze [-hV] <jarPath>
analyzes a JAR file for Kafka Stream usage
      <jarPath>   Path to JAR File
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
```

## Streams Bytecode Analyzer

This tool accepts a JAR file path as its input and outputs all the methods
in the `org.apache.kafka.streams` package that the JAR references. An example
output:

```
org/apache/kafka/streams/StreamsConfig -> [<init>(Map)]
org/apache/kafka/streams/kstream/KStream -> [selectKey(KeyValueMapper), to(String), join(KTable, ValueJoiner), filter(Predicate)]
org/apache/kafka/streams/KafkaStreams -> [<init>(Topology, StreamsConfig), start(), close()]
org/apache/kafka/streams/StreamsBuilder -> [table(String, Materialized), <init>(), stream(String), build()]
```