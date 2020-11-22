package com.github.ehansen31.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        System.out.println("hello world");
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my_fourth_application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

//        consumer.subscribe(Collections.singleton("first_topic"));

        TopicPartition partitionToReadFrom = new TopicPartition("first_topic", 0);
        long offsetToReadFrom = 15l;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessageToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;


        while(keepOnReading){
            ConsumerRecords<String,String> records =  consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record: records){
                numberOfMessagesReadSoFar+=1;
                logger.info("key: " + record.key());
                logger.info("value: " + record.value());
                logger.info("partition: " + record.partition());
                logger.info("offset: " + record.offset());
                if(numberOfMessagesReadSoFar>= numberOfMessageToRead){
                    keepOnReading = false;
                    break;
                }
            }
        }
    }
}
