package com.github.isaachbn.kafka.tutorial_one;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //CreateProducer
        KafkaProducer<String, String> producer = new KafkaProducer(properties);

        //Loop SEND
        for (int i=0; i<10; i++) {
            //CreateProducer Record
            String topic = "first_topic";
            String value = "Helllo number " + i;
            String key = "id_" + i;
            ProducerRecord<String, String> record = new ProducerRecord(topic, key, value);

            //Send data
            producer.send(record, (metadata, exception) -> {
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
            }).get(); //block the send() to make it synchronous -  don't do this in production!
        }
        producer.flush();
        producer.close();
    }
}
