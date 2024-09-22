package com.cb.kafka.produce.ActionFuction;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

public class reduceByKey {
    public static void main(String[] args) {

        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaPairRDD<String, Integer> rdd = sc
                .<String,Integer>parallelizePairs(Arrays.asList(
                        new Tuple2<String, Integer>("a", 1),
                        new Tuple2<String, Integer>("b", 2),
                        new Tuple2<String, Integer>("b", 5),
                        new Tuple2<String, Integer>("c", 3)
                ));

        sc
               .<String,Integer>parallelizePairs(Arrays.asList(
                        new Tuple2<String,Integer>("a", 1),
                        new Tuple2<String,Integer>("b", 2),
                        new Tuple2<String,Integer>("b", 5),
                        new Tuple2<String,Integer>("c", 3)
                )).reduceByKey(Integer::sum).collect().forEach(System.out::println);
//        rdd.reduceByKey(Integer::sum).collect().forEach(System.out::println);
        rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).foreach(System.out::println);

        // 4. 关闭sc
        sc.stop();

    }
}
