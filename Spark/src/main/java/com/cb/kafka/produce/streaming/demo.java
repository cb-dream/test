package com.cb.kafka.produce.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContextState;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import scala.util.control.Exception;

import java.net.URI;
import java.util.*;

public class demo {
    public static void main(String[] args) throws InterruptedException {
        //TODO SparkStreaming 流式数据进行计算 环境封装 模型封装
        JavaStreamingContext sparkStreaming = new JavaStreamingContext(
                new SparkConf()
                        .setMaster("local[*]")
                        .setAppName("SparkStreaming")
                , new Duration(2000L)
        );

        JavaReceiverInputDStream<String> dStream = sparkStreaming.socketTextStream("localhost", 9092);


        dStream.flatMap(l -> Arrays.asList(l.split(" ")).iterator()).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<>(s, 1);
            }
        }).window(new Duration(2000l))
           .reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2)  {
                return integer + integer2;
            }
        });

        dStream.map(new Function<String, String>() {
            @Override
            public String call(String s){
                return s;
            }
        }).filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) {
                return !"a".equals(s);
            }
        }).cache()
          .mapToPair(s -> new Tuple2<>(s , 1))
          .window(new Duration(1000L))
          .reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
              @Override
              public Integer call(Integer integer, Integer integer2)  {
                  return 0;
              }
          },new Duration(1000l));

        dStream.foreachRDD(
                rdd -> {
                    rdd.take(10);
                    rdd.first();
                    rdd.countByValue();
                    rdd.foreachPartition(s -> System.out.println(s + "\t" + s));
                    rdd.foreach(s -> System.out.println(s + "\t" + s));
                    rdd.saveAsTextFile("hdfs://localhost:9000/user/spark/output/");
                    rdd.saveAsObjectFile("hdfs://localhost:9000/user/spark/output/");
                    rdd.collect();
                    rdd.sortBy(s -> s, true, 2);
                }
        );

        HashMap<String, Object> map = new HashMap<>();
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        map.put(ConsumerConfig.GROUP_ID_CONFIG, "cb");
        ArrayList<String> test = new ArrayList<>();
        test.add("test");
        JavaInputDStream<ConsumerRecord<String, String>> directStream =
                KafkaUtils.createDirectStream(
                sparkStreaming,
                LocationStrategies.PreferBrokers(),
                ConsumerStrategies.<String, String>Subscribe(test,map));

        directStream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<String, String> s) {
                return Arrays.asList(s.value().split(" ")).iterator();
            }
        }).mapToPair(s -> new Tuple2<>(s , 1))
                .reduceByKeyAndWindow(null
                        ,Duration.apply(1000L),Duration.apply(1000L))
                .print();


//        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
//                (values, state) -> {
//                    Integer newSum = ...  // add the new values with the previous running count to get the new count
//                    return Optional.of(newSum);
//                };
        // RDD containing spam information
      /*  JavaPairRDD<String, Double> spamInfoRDD = jssc.sparkContext().newAPIHadoopRDD(...);

        JavaPairDStream<String, Integer> cleanedDStream = wordCounts.transform(rdd -> {
            rdd.join(spamInfoRDD).filter(...); // join data stream with spam information to do data cleaning
  ...
        });*/

//        JavaPairRDD<String, String> dataset = ...
//        JavaPairDStream<String, String> windowedStream = stream.window(Durations.seconds(20));
//        JavaPairDStream<String, String> joinedStream = windowedStream.transform(rdd -> rdd.join(dataset));

        new Thread(new MonitorStop(sparkStreaming)).start();

        // 执行流的任务
        sparkStreaming.start();
        sparkStreaming.awaitTermination();

    }

    public static class MonitorStop implements Runnable {

    JavaStreamingContext javaStreamingContext  =null;

        public MonitorStop(JavaStreamingContext sparkStreaming) {
            this.javaStreamingContext = sparkStreaming;
        }

        @Override
        public void run() {
            try {
                FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020"), new Configuration(), "cb");
                while (true) {
                    Thread.sleep(5000);
                        boolean exists = fileSystem.exists(new Path("hdfs://hadoop102:8020/sparkStop"));
                    if(exists){
                        StreamingContextState state = javaStreamingContext.getState();
                        if(state == StreamingContextState.ACTIVE){
                            javaStreamingContext.stop(true,true);
                            System.exit(0);
                        }
                    }
                }
            }catch (java.lang.Exception e){
                e.printStackTrace();
            }
        }
    }
}