package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerExample {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerExample.class);


    public static void main(String[] args) {

        log.info("Kafka stars sending message");

        String bootstrap_servers = "192.168.1.115:9092";

        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap_servers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());


        KafkaProducer<String,String> first_producer =
                new KafkaProducer<>(props);

        ProducerRecord<String, String> record =
                new ProducerRecord<>("kafka-example","Mayıslar Bizim");

        // sending data
        first_producer.send(record);

        //async olduğu için flush ve close yapalım.
        first_producer.flush();
        first_producer.close();

        log.info("kafka finished sending message");






    }
}
