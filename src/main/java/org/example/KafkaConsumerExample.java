package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class KafkaConsumerExample {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerExample.class);

    public static void main(String[] args) {
        log.info("Kafka stars sending message");

        String bootstrap_servers = "192.168.1.115:9092";
        String groupId = "group2";
        String offset_start = "earliest";

        Properties props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap_servers);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                offset_start);


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("kafka-example"));

        while(true)
        {
            ConsumerRecords<String,String> records =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String,String> record: records) {

                System.out.println("Key:" + record.key());
                System.out.println("Value:" + record.value());
                System.out.println("Topic:" + record.topic());
                System.out.println("Partition:" + record.partition());

                SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

                System.out.println("Date:" + sdf.format(new Date(record.timestamp())));

                System.out.println("******************************************************");

            }


        }

    }
}
