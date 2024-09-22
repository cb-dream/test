package com.cb.kafka.produce;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerTransactional {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"transactionalId_producers");

        KafkaProducer kafkaProducer = new KafkaProducer<String, String>(properties);

        kafkaProducer.initTransactions();
        kafkaProducer.beginTransaction();
        try {
            for (int i = 0; i < 7; i++) {
                kafkaProducer.send(new ProducerRecord<>("first", i, "test", "atguigu"));
            }
            kafkaProducer.commitTransaction();
        } catch (Exception e)

    {
        e.printStackTrace();
      kafkaProducer.abortTransaction();
      }finally {
            kafkaProducer.close();
        }
    }
}
