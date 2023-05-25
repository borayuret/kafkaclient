package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerExampleWithCallback {




    public static void main(String[] args) {



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
                new ProducerRecord<>("kafka-example3","Okan Buruk");

        // sending data
        first_producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                Logger logger =
                        LoggerFactory.getLogger(KafkaProducerExampleWithCallback.class);

                if (e == null)
                {
                    System.out.println("Topic:" + recordMetadata.topic());
                    System.out.println("Partition:" + recordMetadata.partition());
                    System.out.println("Offset:" + recordMetadata.offset());
                    System.out.println("Timestamp:" + recordMetadata.timestamp());

                }
                else
                {
                    System.err.println("Hata:" + e.getMessage());
                }
            }
        });

        //async olduğu için flush ve close yapalım.
        first_producer.flush();
        first_producer.close();








    }
}
