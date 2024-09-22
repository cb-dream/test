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

public class windowCount {
    public static void main(String[] args) throws Exception {
        //TODO SparkStreaming 流式数据进行计算 环境封装 模型封装
        JavaStreamingContext sparkStreaming = new JavaStreamingContext(
                new SparkConf()
                        .setMaster("local[*]")
                        .setAppName("SparkStreaming")
                , new Duration(2000L)
        );

        //TODO 对一定范围内对数据统计
        // eg  :  统计一个小时内 每十分钟 雨量 统计

        JavaReceiverInputDStream<String> dStream = sparkStreaming.socketTextStream("localhost", 9090);
//        dStream.print();
        dStream
                .flatMap( l -> Arrays.asList(l.split(" ")).iterator())
                .mapToPair( v -> new Tuple2<String, Integer>(v, 1) )
                // TODO 滑动窗口 windowDuration >   slideDuration
                .window(new Duration(6000L),new Duration(2000L))
                // TODO 滚动窗口 widowDuration  =   slideDuration
                .window(new Duration(6000L),new Duration(6000L))
                // TODO  widowDuration  <   slideDuration
                .window(new Duration(3000L),new Duration(6000L))
                .reduceByKey(Integer::sum)
                .print();

//        dStream.reduceByWindow()

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
