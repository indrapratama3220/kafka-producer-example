package com.kafka.producer;

import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class StreamA {
    private static Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,AppConfigs.applicationIDA);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,AppConfigs.bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream kStream = streamsBuilder.stream(AppConfigs.topicName);

        kStream.foreach((k,v)->System.out.println("Key : " + k + ", Value : "+ v));
        Topology topology = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology,props);
        logger.info("Stream is starting...");
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Stream is shutting down...");
            kafkaStreams.close();
        }));

    }
}
