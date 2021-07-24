package com.github.isaachbn.kafka.tutorial_one;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);
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
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                // executes every time a record is successfully sent or an exception is thrown
                if (exception == null) {
                    //the record was successfully sent
                    logger.info(
                            "Received new metadat. \n " +
                                    "Topic: " + metadata.topic() + "\n " +
                                    "Particion: " + metadata.partition() + " \n " +
                                    "Offset: " + metadata.offset() + " \n " +
                                    "Timestamp: " + metadata.timestamp() + ""
                    );
                } else {
                    logger.error("Error producer", exception);
                }
            }
        });
        producer.flush();
        producer.close();
    }
}
