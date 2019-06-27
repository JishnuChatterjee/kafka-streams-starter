package com.github.jchatt.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {

    public static void main(String[] args) {

        System.out.println("Starting App Kafka-Streams");
        Properties properties=new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());

        StreamsBuilder builder=new StreamsBuilder();
        System.out.println("Created Builder for App Kafka-Streams");
        

        KStream<String,String> wordCountInput=builder.stream("word-count-input");

        System.out.println("Created Kstream for word-count-input topic");
        KTable<String,Long> wordCounts=wordCountInput.mapValues(value->value.toLowerCase())
                        .flatMapValues(value-> Arrays.asList(value.split(" ")))
                        .selectKey((key,value)->value)
                        .groupByKey()
                        .count();
        System.out.println("Created kTable for word-count-input topic");
        wordCounts.toStream().to("word-count-output");

        KafkaStreams kafkaStreams=new KafkaStreams(builder.build(), properties);

        kafkaStreams.start();
    }

}
