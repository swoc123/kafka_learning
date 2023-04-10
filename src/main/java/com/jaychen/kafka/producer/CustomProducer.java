package com.jaychen.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class CustomProducer {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

//        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
//        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
//        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);
//        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
//        properties.put(ProducerConfig.ACKS_CONFIG,"1");
//        properties.put(ProducerConfig.RETRIES_CONFIG,3);

        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"transactional_id_01");


        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties); // key type is String, value type isString
        //value才是 payload

        kafkaProducer.initTransactions();
        kafkaProducer.beginTransaction();
        try {
            kafkaProducer.send(new ProducerRecord<>("transaction_test","value1")).get();
            kafkaProducer.commitTransaction();
        } catch (InterruptedException e) {
            kafkaProducer.abortTransaction();
        } finally {
            kafkaProducer.close();
        }


        kafkaProducer.send(new ProducerRecord<>("first", "money"), new Callback() {
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