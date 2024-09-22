package com.cb.kafka.produce.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class test {
    public static void main(String[] args) throws Exception {
        //TODO SparkStreaming 流式数据进行计算 环境封装 模型封装
        JavaStreamingContext sparkStreaming = new JavaStreamingContext(
                new SparkConf()
                        .setMaster("local[*]")
                        .setAppName("SparkStreaming")
                , new Duration(2000L)
        );

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
