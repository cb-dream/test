package com.cb.kafka.produce.RDDFuction;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class FlatMapTest {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        List<List<Integer>> list = Arrays.asList(
                Arrays.asList(1,2,3,4),
                Arrays.asList(1,2,3,4)
        );
        sc.parallelize(list).flatMap(s -> s.iterator()).collect().forEach(System.out::println);
        sc
                .textFile("data/w*")
                .flatMap(s ->Arrays.asList(s.split(" ")).iterator())
                .collect()
                .forEach(System.out::println);
        // 4. 关闭sc
        sc.stop();
    }
}
