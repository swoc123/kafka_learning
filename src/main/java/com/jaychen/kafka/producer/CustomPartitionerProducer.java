package com.jaychen.kafka.producer;

import java.util.Properties;

import com.jaychen.kafka.producer.config.CustomPartitioner;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class CustomPartitionerProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());


        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties); // key type is String, value type isString
        //value才是 payload

        kafkaProducer.send(new ProducerRecord<>("first",1, "key","money"), new Callback() { // (topic, partition, key, value)
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e==null){
                    System.out.println("topic is "+ recordMetadata.topic()+ "; "+ "partition is "+ recordMetadata.partition());
                }else{
                    e.printStackTrace();
                }
            }
        }); // key can also be a parameter to specify the partition. // Integer partition number can also be a parameter after key, so 4 params together.

        Thread.sleep(2);

        kafkaProducer.close();
    }
}