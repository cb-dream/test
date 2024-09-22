package com.cb.kafka.produce.streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class kafka {
    public static void main(String[] args) throws Exception {
        //TODO SparkStreaming 流式数据进行计算 环境封装 模型封装
        JavaStreamingContext sparkStreaming = new JavaStreamingContext(
                new SparkConf()
                        .setMaster("local[*]")
                        .setAppName("SparkStreaming")
                , new Duration(2000L)
        );

        // 创建配置参数
        HashMap<String, Object> map = new HashMap<>();
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        map.put(ConsumerConfig.GROUP_ID_CONFIG,"cb");
        map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        // 需要消费的主题
        ArrayList<String> topic = new ArrayList<>();
        topic.add("test");

        JavaInputDStream<ConsumerRecord<String, String>> directStream =
                KafkaUtils.createDirectStream(
                        sparkStreaming,
                        LocationStrategies.PreferBrokers(),
                        ConsumerStrategies.<String, String>Subscribe(topic,map));

        directStream.map((Function<ConsumerRecord<String, String>, String>) v1 -> v1.value()).print(100);

        // 启动数据采集器
        //nothing to execute
        sparkStreaming.start();

        // 采集器的终止  （停止运行）
        // 没有数据源进入
        sparkStreaming.awaitTermination();

        // TODO
        //sparkStreaming.close();
    }
}
