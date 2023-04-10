package com.jaychen.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class CustomConsumer {

    public static void main(String[] args) {
        //configuration
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092, hadoop103:9092");

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");



        // new a consumer
        KafkaConsumer kafkaConsumer = new KafkaConsumer<String,String>(properties);

        // define topic
        ArrayList<String> topics = new ArrayList<>();
        topics.add("first");
        kafkaConsumer.subscribe(topics);



//        ASSIGN a specific topic and partition to consume
//        /**
//        ArrayList<TopicPartition> arrayList = new ArrayList<>();
//        arrayList.add(new TopicPartition("first",1));
//        kafkaConsumer.assign(arrayList); */

         // SETTING TO CONSUME FROM A SPECIFIC TOPIC PARTITION AND OFFSET
//        Set<TopicPartition> topicPartitionSet =  kafkaConsumer.assignment();
            //  it takes time to make consumption plan, waiting..
//        while(topicPartitionSet.size()==0){
//            kafkaConsumer.poll(Duration.ofSeconds(1));
//            topicPartitionSet=kafkaConsumer.assignment();
//        }
//
//        for( TopicPartition assignment : topicPartitionSet){
//            kafkaConsumer.seek(assignment,100);
//        }
//        while (true){kafkaConsumer.poll()}


        /** This is to specify to consume data one day ago . Use function: offsetForTimes to convert time to offset
         *
         *         Set<TopicPartition> assignment = kafkaConsumer.assignment();
         *         HashMap<TopicPartition, Long> topicPartitionLongHashMap = new HashMap<>();
         *         for (TopicPartition topicPartition: assignment){
         *             topicPartitionLongHashMap.put(topicPartition,System.currentTimeMillis()-1*24*3600*1000);
         *         }
         *         Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap= kafkaConsumer.offsetsForTimes(topicPartitionLongHashMap);
         *         for(TopicPartition topicPartition:assignment){
         *             OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetAndTimestampMap.get(topicPartition);
         *             kafkaConsumer.seek(topicPartition,offsetAndTimestamp.offset());
         *         }
         */



        // consume data
        while (true) {
            ConsumerRecords<String, String> consumerRecord = kafkaConsumer.poll(Duration.ofSeconds(1)); // pull data per second

            for (ConsumerRecord<String, String> record: consumerRecord){
                System.out.println(record);
            }
            kafkaConsumer.commitAsync(); // async submit offset to internal topic __consumer_offsets
        }


    }
}
     //  to not miss or duplicate data consumption, we should enable atomic characteristics for consuming and commit( submit offset) ; 