package com.github.isaachbn.kafka.tutorial_one;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //CreateProducer
        KafkaProducer<String, String> producer = new KafkaProducer(properties);

        //CreateProducer Record
        ProducerRecord<String, String> record = new ProducerRecord("first_topic", "Send by java!");

        //Send data
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
