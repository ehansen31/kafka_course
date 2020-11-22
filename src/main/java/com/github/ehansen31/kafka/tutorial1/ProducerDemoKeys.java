package com.github.ehansen31.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String key = "id" + Integer.toString(i);
            String value = "hello world " + Integer.toString(i);

            logger.info("key: " + key);

            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, value);
            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("received new metadata:\n" + "topic is: " + recordMetadata.topic()
                                + "\npartition is: " + recordMetadata.partition() + "\noffset is: " + recordMetadata.offset()
                                + "\n timestamp is: " + recordMetadata.timestamp()
                        );
                    } else {
                        logger.error("error while producing", e);
                    }
                }
            }).get();
        }
        producer.close();
    }
}
