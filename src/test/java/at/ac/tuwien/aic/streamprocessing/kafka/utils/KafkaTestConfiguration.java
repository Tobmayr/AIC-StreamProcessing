package at.ac.tuwien.aic.streamprocessing.kafka.utils;

import java.util.Properties;

public class KafkaTestConfiguration {

    private static final int KAFKA_PORT = 9092;
    private static final String KAFKA_URI = "localhost:" + KAFKA_PORT;

    public static Properties createProducerProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_URI);
        properties.put("acks", "1");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return properties;
    }

    public static Properties createConsumerProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_URI);
        properties.put("group.id", "test-group-id");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "10000");
        properties.put("auto.offset.reset", "earliest"); // from-beginning
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return properties;
    }
}
