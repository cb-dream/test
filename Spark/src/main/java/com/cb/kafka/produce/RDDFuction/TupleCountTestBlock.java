package com.cb.kafka.produce.RDDFuction;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TupleCountTestBlock {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码

        List<Tuple2<String,Integer>> tuple2List =
                Arrays.asList(
                        new Tuple2("a",1),
                        new Tuple2("b",2),
                        new Tuple2("b",5),
                        new Tuple2("c",3)
                );

        /*(c,[(c,3)])
            (a,[(a,1)])
            (b,[(b,2), (b,5)])*/
        sc
                .parallelizePairs(tuple2List,3)
                .groupBy(v ->v._1)
                .collect()
                .forEach(System.out::println);
        /*(dataX,[1])
            (flink,[1, 1])
            (java,[1])
            (kafka,[1])
            (maxwell,[1])
            (flume,[1, 1])*/
        sc
                .textFile("data/w*")
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(v -> new Tuple2<>(v,1))
                .groupByKey()
                //.map(v ->new Tuple2(v._1,v._2.spliterator().estimateSize()))
                .collect()
                .forEach(System.out::println);

        System.out.println("====");

        /*(dataX,1)
        (flink,2)
        (java,1)
        (kafka,1)
        (maxwell,1)
        (flume,2)*/
        sc
                .textFile("data/w*")
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(v -> new Tuple2<>(v,1))
                .reduceByKey(Integer::sum, 2).collect().forEach(System.out::println);
        // 4. 关闭sc
        sc.stop();
    }
}
