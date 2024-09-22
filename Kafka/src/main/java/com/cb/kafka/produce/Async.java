package com.cb.kafka.produce;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Async {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

    Properties properties =  new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "0");
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,  StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    KafkaProducer kafkaProducer = new KafkaProducer<String,String>(properties);
        for (int i = 0; i < 10; i++) {
        kafkaProducer.send(new ProducerRecord<>("first","cb"+i), new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null && recordMetadata != null
                ){
                    System.out.println(
                            recordMetadata.topic()+"++++"
                                    +recordMetadata.partition()+"++++"
                                    +recordMetadata.offset()+"++++"
                                    +recordMetadata.timestamp()+"++++"
                    );
                }
            }
        }).get();
    }
        kafkaProducer.close();
}
}
