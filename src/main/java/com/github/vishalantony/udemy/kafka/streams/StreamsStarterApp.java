package com.github.vishalantony.udemy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.Properties;

/**
 * Reads words from input topic and prints the word with a running count to the output topic
 */

public class StreamsStarterApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "WordCount");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        final String INPUT_TOPIC = "streams-plaintext-input";
        final String OUTPUT_TOPIC = "streams-wordcount-output";

        KStream<String, String> wcInput = builder.stream(INPUT_TOPIC); // stream from kafka
        KTable<String, Long> wordCounts = wcInput
                .mapValues((ValueMapper<String, String>) String::toLowerCase) // convert each message(value) to lowercase
                .flatMapValues(value -> Arrays.asList(value.split(" "))) // Split each message into list of words. Each word being a message
                .selectKey((ignoredKey, word) -> word) // Make the key and value same. i.e., the word
                .groupByKey()
                .count(); // Count occurrences of each key

        wordCounts.toStream().to(OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start(); // start the stream application
        System.out.println(streams.toString()); // print the topology
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close)); // Add shutdown hook
    }
}
