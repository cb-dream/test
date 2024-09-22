package com.cb.kafka.produce.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.internal.config.R;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class Fuction {
    public static <U> void main(String[] args) throws Exception {
        //TODO SparkStreaming 流式数据进行计算 环境封装 模型封装
        JavaStreamingContext sparkStreaming = new JavaStreamingContext(
                new SparkConf()
                        .setMaster("local[*]")
                        .setAppName("SparkStreaming")
                , new Duration(2000L)
        );

        // rDD
        // DATASET
        // DSTREAM
        JavaReceiverInputDStream<String> dStream = sparkStreaming.socketTextStream("localhost", 9090);

//        dStream.transform()

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
