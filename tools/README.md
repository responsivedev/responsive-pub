# Responsive Tools

This module contains tools that can be helpful when using the Responsive
Platform. To run a tool in this module execute:
```bash
./gradlew :tools:run --args=<args>
```

## Streams Bytecode Analyzer

This tool accepts a JAR file path as its input and outputs all the methods
in the `org.apache.kafka.streams` package that the JAR references. An example
output:

```
> Task :tools:run
org/apache/kafka/streams/StreamsConfig -> [<init>(Map)]
org/apache/kafka/streams/kstream/KStream -> [selectKey(KeyValueMapper), to(String), join(KTable, ValueJoiner), filter(Predicate)]
org/apache/kafka/streams/KafkaStreams -> [<init>(Topology, StreamsConfig), start(), close()]
org/apache/kafka/streams/StreamsBuilder -> [table(String, Materialized), <init>(), stream(String), build()]
```